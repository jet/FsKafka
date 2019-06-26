namespace Jet.ConfluentKafka.FSharp

open Confluent.Kafka
open Serilog
open System

type PartitionResult =
    | OkReachedZero // check 1
    | WarningLagIncreasing // check 3
    | ErrorPartitionStalled of lag: int64 // check 2
    | Healthy

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
    type [<NoComparison>] ConsumerProgressInfo =
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
        let progress (admin : IAdminClient, consumer:IConsumer<'k,'v>) (topic:string) (ps:int[]) = async {
          let! topicPartitions =
            if ps |> Array.isEmpty then
              async {
                let meta = admin.GetMetadata(topic, TimeSpan.FromSeconds 40.)
                let meta = meta.Topics |> Seq.find(fun t -> t.Topic = topic)
                return meta.Partitions |> Seq.map(fun p -> new TopicPartition(topic, new Partition(p.PartitionId))) }
            else
              async { return ps |> Seq.map(fun p -> new TopicPartition(topic,new Partition(p))) }

          let committedOffsets =
            consumer.Committed(topicPartitions, TimeSpan.FromSeconds(20.))
            |> Seq.sortBy(fun e -> let p = e.Partition in p.Value)
            |> Seq.map(fun e -> let p = e.Partition in p.Value, e)
            |> Map.ofSeq

          let! watermarkOffsets =
            topicPartitions
              |> Seq.map(fun tp -> async {
                return let p = tp.Partition in p.Value, consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds 40.)}
              )
              |> Async.Parallel

          let watermarkOffsets =
            watermarkOffsets
            |> Map.ofArray

          let partitions =
            (watermarkOffsets, committedOffsets)
            ||> Map.mergeChoice (fun p -> function
              | Choice1Of3 (hwo,cOffset) ->
                let e,l,o = (let v = hwo.Low in v.Value),(let v = hwo.High in v.Value),let v = cOffset.Offset in v.Value
                // Consumer offset of (Invalid Offset -1001) indicates that no consumer offset is present.  In this case, we should calculate lag as the high water mark minus earliest offset
                let lag, lead =
                  match o with
                  | offset when offset = let v = Offset.Unset in v.Value -> l - e, 0L
                  | _ -> l - o, o - e
                { partition = p ; consumerOffset = cOffset.Offset ; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = lag ; lead = lead ; messageCount = l - e }
              | Choice2Of3 hwo ->
                // in the event there is no consumer offset present, lag should be calculated as high watermark minus earliest
                // this prevents artifically high lags for partitions with no consumer offsets
                let e,l = (let v = hwo.Low in v.Value),let v = hwo.High in v.Value
                { partition = p ; consumerOffset = Offset.Unset; earliestOffset = hwo.Low ; highWatermarkOffset = hwo.High ; lag = l - e ; lead = 0L ; messageCount = l - e }
                //failwithf "unable to find consumer offset for topic=%s partition=%i" topic p
              | Choice3Of3 o ->
                let invalid = Offset.Unset
                { partition = p ; consumerOffset = o.Offset ; earliestOffset = invalid ; highWatermarkOffset = invalid ; lag = invalid.Value ; lead = invalid.Value ; messageCount = -1L })
            |> Seq.map (fun kvp -> kvp.Value)
            |> Seq.toArray

          return {
            topic = topic ; group = consumer.Name ; partitions = partitions
            totalLag = partitions |> Seq.sumBy (fun p -> p.lag)
            minLead =
              if partitions.Length > 0 then
                partitions |> Seq.map (fun p -> p.lead) |> Seq.min
              else let v = Offset.Unset in v.Value }}
    
    type OffsetValue =
        | Unset
        | Valid of offset: int64
        static member ofOffset(offset : Offset) =
            match offset.Value with
            | _ when offset = Offset.Unset -> Unset
            | valid -> Valid valid
        override this.ToString() =
            match this with
            | Unset -> "Unset"
            | Valid value -> value.ToString()

    type PartitionInfo =
        {   partition : int
            consumerOffset : OffsetValue
            earliestOffset : OffsetValue
            highWatermarkOffset : OffsetValue
            lag : int64 }
        static member ofConsumerPartitionProgressInfo(info : ConsumerPartitionProgressInfo) = {
            partition = info.partition
            consumerOffset = OffsetValue.ofOffset info.consumerOffset
            earliestOffset = OffsetValue.ofOffset info.earliestOffset
            highWatermarkOffset = OffsetValue.ofOffset info.highWatermarkOffset
            lag = info.lag }

    [<NoComparison>]
    type Window = Window of PartitionInfo []

    let createPartitionInfoList (info : ConsumerProgressInfo) =
        Window (Array.map PartitionInfo.ofConsumerPartitionProgressInfo info.partitions)

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

        member __.SafeTryFullClone() =
            lock lockObj (fun () -> if size = capacity then copy() |> Some else None)

        member __.SafeAdd (x : 'A) =
            lock lockObj (fun _ -> add x)

        member __.Reset () =
            lock lockObj (fun _ ->
                head <- 0
                tail <- -1
                size <- 0)

    module Rules =

        // Rules taken from https://github.com/linkedin/Burrow
        // Rule 1:  If over the stored period, the lag is ever zero for the partition, the period is OK
        // Rule 2:  If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
        // Rule 3:  If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)

        // The following rules are not implementable given our poll based implementation - they should also not be needed
        // Rule 4:  If the difference between now and the lastPartition offset timestamp is greater than the difference between the lastPartition and firstPartition offset timestamps, the
        //          consumer has stopped committing offsets for that partition (error), unless
        // Rule 5:  If the lag is -1, this is a special value that means there is no broker offset yet. Consider it good (will get caught in the next refresh of topics)

        // If lag is ever zero in the window, no other checks needed
        let checkRule1 (partitionInfoWindow : PartitionInfo []) =
            partitionInfoWindow |> Array.exists (fun i -> i.lag = 0L)

        // If there is lag, the offsets should be progressing in window
        let checkRule2 (partitionInfoWindow : PartitionInfo []) =
            let offsetsIndicateLag (firstConsumerOffset : OffsetValue) (lastConsumerOffset : OffsetValue) =
                match (firstConsumerOffset, lastConsumerOffset) with
                | Valid validFirst, Valid validLast ->
                    validLast - validFirst <= 0L
                | Unset, Valid _ ->
                    // Partition got its initial offset value this window, check again next window.
                    false
                | Valid _, Unset ->
                    // Partition somehow lost its offset in this window, something's probably wrong.
                    true
                | Unset, Unset ->
                    // Partition has invalid offsets for the entire window, there may be lag.
                    true

            let firstWindowPartitions = partitionInfoWindow |> Array.head
            let lastWindowPartitions = partitionInfoWindow |> Array.last

            let checkPartitionForLag (firstWindowPartition : PartitionInfo) (lastWindowPartition : PartitionInfo)  =
                match lastWindowPartition.lag with
                | 0L -> None
                | lastPartitionLag when offsetsIndicateLag firstWindowPartition.consumerOffset lastWindowPartition.consumerOffset ->
                    if lastWindowPartition.partition <> firstWindowPartition.partition then failwithf "Partitions did not match in rule2"
                    Some lastPartitionLag
                | _ -> None

            checkPartitionForLag firstWindowPartitions lastWindowPartitions

        // Has the lag reduced between steps in the window
        let checkRule3 (partitionInfoWindow : PartitionInfo []) =
            let lagDecreasing =
                partitionInfoWindow
                |> Seq.pairwise
                |> Seq.exists (fun (prev, curr) -> curr.lag < prev.lag)

            not lagDecreasing

        let checkRulesForPartition (partitionInfoWindow : PartitionInfo []) =
            if checkRule1 partitionInfoWindow then OkReachedZero else

            match checkRule2 partitionInfoWindow with
            | Some lag ->
                ErrorPartitionStalled lag
            | None when checkRule3 partitionInfoWindow ->
                WarningLagIncreasing
            | _ ->
                Healthy

        let checkRulesForAllPartitions (windows : Window []) =
            windows
            |> Seq.collect (fun (Window partitionInfo) -> partitionInfo)
            |> Seq.groupBy (fun p -> p.partition)
            |> Seq.map (fun (p, info) -> p, checkRulesForPartition (Array.ofSeq info))

    module private Logging =

        let logResults (log : ILogger) topic consumerGroup (partitionResults : (int * PartitionResult) seq) =
            let cat = function
                | OkReachedZero | Healthy -> Choice1Of3 ()
                | ErrorPartitionStalled _lag -> Choice2Of3 ()
                | WarningLagIncreasing -> Choice3Of3 ()
            match partitionResults |> Seq.groupBy (snd >> cat) |> List.ofSeq with
            | [ Choice1Of3 (), _ ] -> log.Information("Monitoring... {topic}/{groupId} Healthy", topic, consumerGroup)
            | errs ->
                for res in errs do
                    match res with
                    | Choice1Of3 (), _ -> ()
                    | Choice2Of3 (), errs ->
                        let lag = function (partitionId, ErrorPartitionStalled lag) -> Some (partitionId,lag) | x -> failwithf "mismapped %A" x
                        log.Error("Monitoring... {topic}/{groupId} Stalled with backlogs on {@stalled} [(partition,lag)]", topic, consumerGroup, errs |> Seq.choose lag)
                    | Choice3Of3 (), warns -> 
                        log.Warning("Monitoring... {topic}/{groupId} Growing lags on {@partitionIds}", topic, consumerGroup, warns |> Seq.map fst)

    let topicPartitionIsForTopic (topic : string) (topicPartition : TopicPartition) =
        topicPartition.Topic = topic

    let private queryConsumerProgress (admin, consumer : IConsumer<'k,'v>) (topic : string) = async {
        let partitionIds = [| for t in consumer.Assignment do if topicPartitionIsForTopic topic t then yield let v = t.Partition in v.Value |] 
        let! r = ConsumerInfo.progress (admin,consumer) topic partitionIds
        return createPartitionInfoList r }

    let private logLatest (logger : ILogger) (topic : string) (consumerGroup : string) (Window partitionInfos) =
        let partitionOffsets =
            partitionInfos
            |> Seq.sortBy (fun p -> p.partition)
            |> Seq.map (fun p -> p.partition, p.highWatermarkOffset, p.consumerOffset)

        let aggregateLag = partitionInfos |> Seq.sumBy (fun p -> p.lag)

        logger.Information("Monitoring... {topic}/{consumerGroup} lag {lag} offsets {offsets}",
            topic, consumerGroup, aggregateLag, partitionOffsets)

    let private monitor consumer (maxFailCount, sleepMs) (buffer : RingBuffer<_>) (logger : ILogger) (topic : string) (consumerGroup : string) onStatus =
        let checkConsumerProgress () = async {
            let! res = queryConsumerProgress consumer topic
            buffer.SafeAdd res
            logLatest logger topic consumerGroup res
            match buffer.SafeTryFullClone() with
            | None -> ()
            | Some ci ->
                let states = Rules.checkRulesForAllPartitions ci |> List.ofSeq
                onStatus topic consumerGroup states }

        let rec loop failCount = async {
            let! failCount = async {
                try do! checkConsumerProgress()
                    return 0
                with exn ->
                    logger.Warning(exn, "Monitoring... {topic}/{consumerGroup} Exception # {failCount}", topic, consumerGroup, failCount)
                    if failCount >= maxFailCount then
                        return raise exn
                    return failCount + 1
            }
            do! Async.Sleep sleepMs
            return! loop failCount }
        loop 0

    type Monitor<'k,'v>(log : ILogger, admin, consumer : IConsumer<'k,'v>, topic, consumerGroup, maxFailCount, intervalMs, windowSize) =
        let ringBuffer = new RingBuffer<_>(windowSize)

        let onStatus = new Event<_>()
        [<CLIEvent>]
        member __.OnStatus = onStatus.Publish

        member __.OnAssigned topicPartitions =
            // Reset the ring buffer for this topic if there's a rebalance for the topic.
            if topicPartitions |> Seq.exists (topicPartitionIsForTopic topic) then
                ringBuffer.Reset()

        member __.Pump =
            let onStatus topic group xs =
                onStatus.Trigger xs
                Logging.logResults log topic group xs
            monitor (admin, consumer) (maxFailCount, intervalMs) ringBuffer log topic consumerGroup onStatus

[<NoComparison; NoEquality>]
type KafkaMonitorConfig =
    {   maxFailCount : int
        pollInterval : TimeSpan
        windowSize : int
        onStatus : (int * PartitionResult) list -> unit
        onFaulted : (exn -> Async<unit>) option } with
    static member Create(?onStatus, ?maxFailCount, ?pollInterval, ?windowSize, ?onFaulted) =
        {    pollInterval = defaultArg pollInterval (TimeSpan.FromSeconds 30.)
             windowSize = defaultArg windowSize 60
             maxFailCount = defaultArg maxFailCount 3
             onStatus = defaultArg onStatus ignore
             onFaulted = onFaulted }