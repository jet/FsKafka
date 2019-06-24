namespace Jet.ConfluentKafka.FSharp

open Confluent.Kafka
open Serilog
open System
    
module Impl =
    module Map =
        let mergeChoice (f:'a -> Choice<'b * 'c, 'b, 'c> -> 'd) (map1:Map<'a, 'b>) (map2:Map<'a, 'c>) : Map<'a, 'd> =
          Set.union (map1 |> Seq.map (fun k -> k.Key) |> set) (map2 |> Seq.map (fun k -> k.Key) |> set)
          |> Seq.map (fun k ->
            match Map.tryFind k map1, Map.tryFind k map2 with
            | Some b, Some c -> k, f k (Choice1Of3 (b,c))
            | Some b, None   -> k, f k (Choice2Of3 b)
            | None,   Some c -> k, f k (Choice3Of3 c)
            | None,   None   -> failwith "invalid state")
          |> Map.ofSeq
      
    /// Progress information for a consumer in a group.
    type ConsumerProgressInfo =
        {   /// The consumer group id.
            group : string

            /// The name of the kafka topic.
            topic : string

            /// Progress info for each partition.
            partitions : ConsumerPartitionProgressInfo[]

            /// The total lag across all partitions.
            totalLag : int64

            /// The minimum lead across all partitions.
            minLead : int64 }
    /// Progress information for a consumer in a group, for a specific topic-partition.
    and [<NoComparison>] ConsumerPartitionProgressInfo =
        {   /// The partition id within the topic.
            partition : int

            /// The consumer's current offset.
            consumerOffset : Offset

            /// The offset at the current start of the topic.
            earliestOffset : Offset

            /// The offset at the current end of the topic.
            highWatermarkOffset : Offset

            /// The distance between the high watermark offset and the consumer offset.
            lag : int64

            /// The distance between the consumer offset and the earliest offset.
            lead : int64

            /// The number of messages in the partition.
            messageCount : int64 }

    /// Operations for providing consumer progress information.
    module ConsumerInfo =
        
        /// Returns consumer progress information.
        /// Passing empty set of partitions returns information for all partitions.
        /// Note that this does not join the group as a consumer instance
        let progress (admin : IAdminClient, consumer:IConsumer<_,_>) (topic:string) (ps:int[]) = async {
          let! topicPartitions =
            if ps |> Array.isEmpty then
              async {
                let meta =
                  admin
                      .GetMetadata((*false,*) TimeSpan.FromSeconds(40.0)).Topics
                  |> Seq.find(fun t -> t.Topic = topic)

                return
                  meta.Partitions
                  |> Seq.map(fun p -> new TopicPartition(topic, new Partition(p.PartitionId)))
                   }
            else
              async { return ps |> Seq.map(fun p -> new TopicPartition(topic,new Partition(p))) }

          let committedOffsets =
            consumer.Committed(topicPartitions, TimeSpan.FromSeconds(20.))
            |> Seq.sortBy(fun e -> e.Partition.Value)
            |> Seq.map(fun e -> e.Partition.Value, e)
            |> Map.ofSeq

          let! watermarkOffsets =
            topicPartitions
              |> Seq.map(fun tp -> async {
                return tp.Partition.Value, consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds 40.)}
              )
              |> Async.Parallel

          let watermarkOffsets =
            watermarkOffsets
            |> Map.ofArray

          let partitions =
            (watermarkOffsets, committedOffsets)
            ||> Map.mergeChoice (fun p -> function
              | Choice1Of3 (hwo,cOffset) ->
                let e,l,o = hwo.Low.Value,hwo.High.Value,cOffset.Offset.Value
                // Consumer offset of (Invalid Offset -1001) indicates that no consumer offset is present.  In this case, we should calculate lag as the high water mark minus earliest offset
                let lag, lead =
                  match o with
                  | offset when offset = Offset.Unset.Value -> l - e, 0L
                  | _ -> l - o, o - e
                { partition = p ; consumerOffset = cOffset.Offset ; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = lag ; lead = lead ; messageCount = l - e }
              | Choice2Of3 hwo ->
                // in the event there is no consumer offset present, lag should be calculated as high watermark minus earliest
                // this prevents artifically high lags for partitions with no consumer offsets
                let e,l = hwo.Low.Value,hwo.High.Value
                { partition = p ; consumerOffset = Offset.Unset; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = l - e ; lead = 0L ; messageCount = l - e }
                //failwithf "unable to find consumer offset for topic=%s partition=%i" topic p
              | Choice3Of3 o ->
                let invalid = Offset.Unset
                { partition = p ; consumerOffset = o.Offset ; earliestOffset = invalid ; highWatermarkOffset = invalid ; lag = invalid.Value ; lead = invalid.Value ; messageCount = -1L })
            |> Seq.map (fun kvp -> kvp.Value)
            |> Seq.toArray

          return
            {
            topic = topic ; group = consumer.Name ; partitions = partitions ;
            totalLag = partitions |> Seq.sumBy (fun p -> p.lag)
            minLead =
              if partitions.Length > 0 then
                partitions |> Seq.map (fun p -> p.lead) |> Seq.min
              else Offset.Unset.Value }}
    
    type PartitionResultKey =
    | NoErrorKey
    | Rule2ErrorKey
    | Rule3ErrorKey

    type PartitionResult =
        | NoError
        | Rule2Error of int64
        | Rule3Error
    with
        static member toKey = function
            | NoError -> NoErrorKey
            | Rule2Error _ -> Rule2ErrorKey
            | Rule3Error -> Rule3ErrorKey
        static member toString = function
            | NoError ->  ""
            | Rule2Error lag -> string lag
            | Rule3Error -> ""

    // rdkafka uses -1001 to represent an invalid offset (usually means the consumer has not committed offsets for that partition)
    let [<Literal>] rdkafkaInvalidOffset = -1001L

    type OffsetValue =
        | Missing
        | Valid of int64
    with
        static member ofOffset(offset : Offset) =
            match offset.Value with
            | value when value = rdkafkaInvalidOffset -> Missing
            | valid -> Valid valid
        override this.ToString() =
            match this with
            | Missing -> "Missing"
            | Valid value -> value.ToString()

    [<NoComparison>]
    type Window = Window of ConsumerPartitionProgressInfo[]

    let createPartitionInfoList (info : ConsumerProgressInfo) =
        Window info.partitions

    // Naive insert and copy out buffer
    type private RingBuffer<'A> (capacity : int) =
        let lockObj = obj()
        let mutable head = 0
        let mutable tail = -1
        let mutable size = 0
        let buffer : 'A [] = Array.zeroCreate capacity

        let copy () =
            let arr = Array.zeroCreate size
            let mutable i = head
            for x = 0 to size - 1 do
                arr.[x] <- buffer.[i % capacity]
                i <- i + 1
            arr

        let add (x : 'A) =
            tail <- (tail + 1) % capacity
            buffer.[tail] <- x
            if (size < capacity) then
                size <- size + 1
            else
                head <- (head + 1) % capacity

        member __.SafeFullClone () =
            lock lockObj (fun () -> if size = capacity then copy() else [||])

        member __.SafeAdd (x : 'A) =
            lock lockObj (fun _ -> add x)

        member __.Reset () =
            lock lockObj (fun _ ->
                head <- 0
                tail <- -1
                size <- 0)

    module private Rules =

        // Rules taken from https://github.com/linkedin/Burrow
        // Rule 1:  If over the stored period, the lag is ever zero for the partition, the period is OK
        // Rule 2:  If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
        // Rule 3:  If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)

        // The following rules are not implementable given our poll based implementation - they should also not be needed
        // Rule 4:  If the difference between now and the lastPartition offset timestamp is greater than the difference between the lastPartition and firstPartition offset timestamps, the
        //          consumer has stopped committing offsets for that partition (error), unless
        // Rule 5:  If the lag is -1, this is a special value that means there is no broker offset yet. Consider it good (will get caught in the next refresh of topics)

        // If lag is ever zero in the window
        let checkRule1 (partitionInfoWindow : ConsumerPartitionProgressInfo []) =
            if partitionInfoWindow |> Array.exists (fun i -> i.lag = 0L)
            then Some NoError
            else None

        // If there is lag, the offsets should be progressing in window
        let checkRule2 (partitionInfoWindow : ConsumerPartitionProgressInfo []) =
            let offsetsIndicateLag (firstConsumerOffset : OffsetValue) (lastConsumerOffset : OffsetValue) =
                match (firstConsumerOffset, lastConsumerOffset) with
                | Valid validFirst, Valid validLast ->
                    validLast - validFirst <= 0L
                | Missing, Valid _ ->
                    // Partition got its initial offset value this window, check again next window.
                    false
                | Valid _, Missing ->
                    // Partition somehow lost its offset in this window, something's probably wrong.
                    true
                | Missing, Missing ->
                    // Partition has invalid offsets for the entire window, there may be lag.
                    true

            let firstWindowPartitions = partitionInfoWindow |> Array.head
            let lastWindowPartitions = partitionInfoWindow |> Array.last

            let checkPartitionForLag (firstWindowPartition : ConsumerPartitionProgressInfo) (lastWindowPartition : ConsumerPartitionProgressInfo)  =
                match lastWindowPartition.lag with
                | 0L -> None
                | lastPartitionLag when offsetsIndicateLag (OffsetValue.ofOffset firstWindowPartition.consumerOffset) (OffsetValue.ofOffset lastWindowPartition.consumerOffset) ->
                    if lastWindowPartition.partition <> firstWindowPartition.partition then failwithf "Partitions did not match in rule2"
                    Some (Rule2Error lastPartitionLag)
                | _ -> None

            checkPartitionForLag firstWindowPartitions lastWindowPartitions

        // Has the lag reduced between steps in the window
        let checkRule3 (partitionInfoWindow : ConsumerPartitionProgressInfo []) =
            let lagDecreasing =
                partitionInfoWindow
                |> Seq.pairwise
                |> Seq.exists (fun (prev, curr) -> curr.lag < prev.lag)

            if lagDecreasing
            then None
            else Some Rule3Error

        let checkRulesForPartition (partitionInfoWindow : ConsumerPartitionProgressInfo []) =
            checkRule1 partitionInfoWindow
            |> Option.orElse (checkRule2 partitionInfoWindow)
            |> Option.orElse (checkRule3 partitionInfoWindow)
            |> Option.defaultValue NoError

        let checkRulesForAllPartitions (windows : Window []) =
            windows
            |> Array.collect (fun (Window partitionInfo) -> partitionInfo)
            |> Array.groupBy (fun p -> p.partition)
            |> Array.map (fun (p, info) -> (p, checkRulesForPartition info))

    module private Logging =

        let logResults (log : ILogger) consumerGroup topic (partitionResults : (int * PartitionResult) []) =
            let mapSnd f (x,y)= x,f y

            let results = partitionResults |> Array.groupBy (snd >> PartitionResult.toKey)

            let logErrorByKey = function
                | (NoErrorKey, _) -> ()
                | (Rule2ErrorKey, errors) -> 
                    errors
                    |> Array.map (mapSnd PartitionResult.toString)
                    |> fun sp -> log.Error("Lag present and offsets not progressing (partition,lag)|consumerGroup={consumerGroup}|topic={topic}|stalledPartitions={@stalledPartitions}", consumerGroup, topic, sp)
                | (Rule3ErrorKey, errors) -> 
                    errors
                    |> Array.map (mapSnd PartitionResult.toString)
                    |> fun lp -> log.Error("Consumer lag is consistently increasing|consumerGroup={consumerGroup}|topic{topic}|laggingPartitions={@laggingPartitions}", consumerGroup, topic, lp)

            match results with
            | [|NoErrorKey, _|] ->
                log.Information("Consumer seems OK|consumerGroup={0}|topic={1}", consumerGroup, topic)
            | errors ->
                errors |> Array.iter logErrorByKey

    let topicPartitionIsForTopic (topic : string) (topicPartition : TopicPartition) =
        topicPartition.Topic = topic

    let private queryConsumerProgress (admin, consumer : IConsumer<_,_>) (topic : string) =
        consumer.Assignment
        |> Seq.filter (topicPartitionIsForTopic topic)
        |> Seq.map (fun topicPartition -> topicPartition.Partition.Value)
        |> Seq.toArray
        |> ConsumerInfo.progress (admin,consumer) topic
        |> Async.map createPartitionInfoList

    let private logLatest (logger : ILogger) (consumerGroup : string) (topic : string) (Window partitionInfos) =
        let partitionOffsets =
            partitionInfos
            |> Seq.sortBy (fun p -> p.partition)
            |> Seq.map (fun p -> p.partition, p.highWatermarkOffset, p.consumerOffset)

        let aggregateLag = partitionInfos |> Seq.sumBy (fun p -> p.lag)

        logger.Information("Consumer info|consumerGroup={0}|topic={1}|lag={2}|offsets={3}", consumerGroup, topic, aggregateLag, partitionOffsets)

    let private monitor consumer (maxFailCount, sleepMs) (buffer : RingBuffer<_>) (logger : ILogger) (topic : string) (consumerGroup : string) handleErrors =
        let checkConsumerProgress () =
            queryConsumerProgress consumer topic
            |> Async.map (fun res ->
                buffer.SafeAdd res
                logLatest logger consumerGroup topic res
                let infoWindow = buffer.SafeFullClone()
                match infoWindow with
                | [||] -> ()
                | ci ->
                    Rules.checkRulesForAllPartitions ci
                    |> handleErrors consumerGroup topic)

        let handleFailure failCount exn =
            logger.Warning("Problem getting ConsumerProgress|consumerGroup={group}|topic={topic}|failCount={failCount}", consumerGroup, topic, failCount)
            if failCount >= maxFailCount then
                raise exn
            failCount + 1

        let rec monitor failCount = async {
            let! failCount = async {
                try
                    do! checkConsumerProgress()
                    return 0
                with
                | exn -> return handleFailure failCount exn
            }
            do! Async.Sleep sleepMs
            return! monitor failCount
        }
        monitor 0

    type Monitor(log : ILogger, admin, consumer, topic, consumerGroup, maxFailCount, intervalMs, windowSize) =
       let ringBuffer = new RingBuffer<_>(windowSize)
       
       member __.OnAssigned topicPartitions =
          // Reset the ring buffer for this topic if there's a rebalance for the topic.
          if topicPartitions |> Seq.exists (topicPartitionIsForTopic topic) then
              ringBuffer.Reset()
                
       member __.Pump() =
           monitor (admin, consumer) (maxFailCount, intervalMs) ringBuffer log topic consumerGroup (Logging.logResults log)

type MonitorConfig = { maxFailCount : int; pollInterval : TimeSpan; windowSize : int } with
    static member Create(?maxFailCount, ?pollInterval, ?windowSize) =
        {    pollInterval = defaultArg pollInterval (TimeSpan.FromSeconds 30.)
             windowSize = defaultArg windowSize 60
             maxFailCount = defaultArg maxFailCount 3 }