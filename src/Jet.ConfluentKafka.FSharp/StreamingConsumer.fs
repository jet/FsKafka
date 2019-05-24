namespace Jet.ConfluentKafka.FSharp

open Confluent.Kafka
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Helpers =

    /// Gathers stats relating to how many items of a given category have been observed
    type PartitionStats() =
        let partitions = Dictionary<int,int64>()
        member __.Ingest(partition,?weight) = 
            let weight = defaultArg weight 1L
            match partitions.TryGetValue partition with
            | true, catCount -> partitions.[partition] <- catCount + weight
            | false, _ -> partitions.[partition] <- weight
        member __.Raw : seq<KeyValuePair<_,_>> = partitions :> _
        member __.Clear() = partitions.Clear()
    #if NET461
        member __.StatsDescending = partitions |> Seq.map (|KeyValue|) |> Seq.sortBy (fun (_,s) -> -s)
    #else
        member __.StatsDescending = partitions |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd
    #endif

    /// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
    let intervalCheck (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            let due = timer.ElapsedMilliseconds > max
            if due then timer.Restart()
            due

    /// Maintains a Stopwatch used to drive a periodic loop which progressively waits 
    /// - `None` if the interval has expired (restarting the timer)
    /// - `Some remainder` if the interval has time remaining
    let intervalTimer (period : TimeSpan) =
        let timer = Stopwatch.StartNew()
        fun () ->
            match period - timer.Elapsed with
            | remainder when remainder.Ticks > 0L -> Some remainder
            | _ -> timer.Restart(); None

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    /// Can't figure out a cleaner way to shim it :(
    let tryPeek (x : Queue<_>) = if x.Count = 0 then None else Some (x.Peek())

module Scheduling =

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type Dispatcher(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let dop = new SemaphoreSlim(maxDop)
        let dispatch inner = async {
            do! inner
            dop.Release() |> ignore } 
        member __.HasCapacity =
            dop.CurrentCount > 0
        member __.TryAdd item =
            if dop.Wait 0 then
                work.Add item
                true
            else false
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }

    /// A Batch of messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'M> = { partitionIndex : int; onCompletion: unit -> unit; messages: 'M [] }

    /// Shared batch state distributed along with each call
    [<NoComparison; NoEquality>]
    type WipBatch<'M> =
        {   mutable elapsedMs : int64 // accumulated processing time for stats
            mutable remaining : int // number of outstanding completions; 0 => batch is eligible for completion
            mutable faults : ConcurrentStack<exn> // exceptions
            batch: Batch<'M> } with
        // need to record stats first as remaining = 0 is used as completion gate
        member private __.RecordDurationFirst(duration : TimeSpan) =
            Interlocked.Add(&__.elapsedMs, int64 duration.TotalMilliseconds + 1L) |> ignore
        member private __.RecordOk(duration : TimeSpan) =
            __.RecordDurationFirst duration
            Interlocked.Decrement(&__.remaining) |> ignore
        member private __.RecordExn(_duration,exn) =
            // Do not record duration or it will mess stats in current implementation //__.RecordDurationFirst duration
            __.faults.Push exn
        static member Create(batch : Batch<'M>, handle) : WipBatch<'M> * seq<Async<unit>> =
            let x = { elapsedMs = 0L; remaining = batch.messages.Length; faults = ConcurrentStack(); batch = batch }
            let mkDispatcher item = async {
                let sw = Stopwatch.StartNew()
                let! res = handle item
                let elapsed = sw.Elapsed
                match res with
                | Choice1Of2 () -> x.RecordOk elapsed
                | Choice2Of2 exn -> x.RecordExn(elapsed,exn) }
            x, Seq.map mkDispatcher batch.messages
    let (|Busy|Completed|Faulted|) = function
        | { remaining = 0; elapsedMs = ms } -> Completed (TimeSpan.FromMilliseconds <| float ms)
        | { faults = f } when not f.IsEmpty -> Faulted (f.ToArray())
        | _ -> Busy

    type Engine<'M>(log : ILogger, handle, tryDispatch : (Async<unit>) -> bool, statsInterval, ?logExternalStats) =
        let incoming = ConcurrentQueue<Batch<'M>>()
        let waiting = Queue<Async<unit>>(1024)
        let activeByPartition = Dictionary<int,Queue<WipBatch<'M>>>()
        let mutable cycles, processingDuration = 0, TimeSpan.Zero
        let startedBatches, completedBatches, startedItems, completedItems = PartitionStats(), PartitionStats(), PartitionStats(), PartitionStats()
        let dumpStats () =
            let startedB, completedB = Array.ofSeq startedBatches.StatsDescending, Array.ofSeq completedBatches.StatsDescending
            let startedI, completedI = Array.ofSeq startedItems.StatsDescending, Array.ofSeq completedItems.StatsDescending
            let totalItemsCompleted = Array.sumBy snd completedI
            let latencyMs = match totalItemsCompleted with 0L -> null | cnt -> box (processingDuration.TotalMilliseconds / float cnt)
            log.Information("Scheduler {cycles} cycles Started {startedBatches}b {startedItems}i Completed {completedBatches}b {completedItems}i latency {completedLatency:f1}ms Ready {readyitems} Waiting {waitingBatches}b",
                cycles, Array.sumBy snd startedB, Array.sumBy snd startedI, Array.sumBy snd completedB, totalItemsCompleted, latencyMs, waiting.Count, incoming.Count)
            let active =
                seq { for KeyValue(pid,q) in activeByPartition -> pid, q |> Seq.sumBy (fun x -> x.remaining) }
                |> Seq.filter (fun (_,snd) -> snd <> 0)
                |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Partitions Active items {@active} Started batches {@startedBatches} items {@startedItems} Completed batches {@completedBatches} items {@completedItems}",
                active, startedB, startedI, completedB, completedI)
            cycles <- 0; startedBatches.Clear(); completedBatches.Clear(); startedItems.Clear(); completedItems.Clear(); processingDuration <- TimeSpan.Zero
            logExternalStats |> Option.iter (fun f -> f log)
        let maybeLogStats : unit -> bool =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats (); true else false
        let drainCompleted (cts : CancellationTokenSource, tcs : TaskCompletionSource<unit>) =
            let mutable more, worked = true, false
            while more do
                more <- false
                for queue in activeByPartition.Values do
                    match tryPeek queue with
                    | None // empty
                    | Some Busy -> () // still working
                    | Some (Faulted exns) ->
                        let effExn = AggregateException(exns).Flatten()
                        // outer layers will react to this by tearing us down
                        // this is no reason not to continue processing other partitions
                        cts.Cancel()
                        tcs.TrySetException(effExn) |> ignore
                    | Some (Completed batchProcessingDuration) ->
                        let partitionId, markCompleted, itemCount =
                            let { batch = { partitionIndex = p; onCompletion = f; messages = msgs } } = queue.Dequeue()
                            p, f, msgs.LongLength
                        completedBatches.Ingest partitionId
                        completedItems.Ingest(partitionId, itemCount)
                        processingDuration <- processingDuration.Add batchProcessingDuration
                        markCompleted ()
                        worked <- true
                        more <- true // vote for another iteration as the next one could already be complete too. Not looping inline/immediately to give others partitions equal credit
            worked
        let tryPrepareNext () =
            match incoming.TryDequeue() with
            | false, _ -> false
            | true, ({ partitionIndex = pid; messages = msgs} as batch) ->
                startedBatches.Ingest(pid)
                startedItems.Ingest(pid, msgs.LongLength)
                let wipBatch, runners = WipBatch.Create(batch, handle)
                runners |> Seq.iter waiting.Enqueue
                match activeByPartition.TryGetValue pid with
                | false, _ -> let q = Queue(1024) in activeByPartition.[pid] <- q; q.Enqueue wipBatch
                | true, q -> q.Enqueue wipBatch
                true
        let queueWork () =
            let mutable more, worked = true, false
            while more do
                match tryPeek waiting with
                | None -> // Crack open a new batch if we don't have anything ready
                    more <- tryPrepareNext ()
                | Some pending -> // Dispatch until we reach capacity if we do have something
                    if tryDispatch pending then
                        worked <- true
                        waiting.Dequeue() |> ignore
                    else // Stop when it's full up
                        more <- false 
            worked

        member __.Pump(cts : CancellationTokenSource, tcs : TaskCompletionSource<unit>) = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let hadResults = drainCompleted (cts, tcs)
                let queuedWork = queueWork ()
                let loggedStats = maybeLogStats ()
                if not hadResults && not queuedWork && not loggedStats then
                    Thread.Sleep 1 } // not Async.Sleep, we like this context and/or cache state if nobody else needs it
        member __.Submit(batches : Batch<'M>) =
            incoming.Enqueue batches

module Submission =

    /// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
    [<NoComparison>]
    type PartitionQueue<'M> = { submissions: SemaphoreSlim; queue : Queue<Scheduling.Batch<'M>> } with
        member __.Append(batch) = __.queue.Enqueue batch
        static member Create(maxSubmits, batch) = let t = { submissions = new SemaphoreSlim(maxSubmits); queue = Queue(maxSubmits * 2) } in t.Append(batch); t 

    /// Holds the stream of incoming batches, grouping by partition
    /// Manages the submission of batches into the Scheduler in a fair manner
    type Engine<'M>(log : ILogger, pumpIntervalMs : int, maxSubmitsPerPartition, submit : Scheduling.Batch<'M> -> unit, statsInterval) =
        let incoming = new BlockingCollection<Scheduling.Batch<'M>[]>(ConcurrentQueue())
        let buffer = Dictionary<int,PartitionQueue<'M>>()
        let mutable cycles, ingested = 0, 0
        let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()
        let dumpStats () =
            let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then yield x.Key, x.Value.queue.Count } |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Submitter {cycles} cycles Ingested {ingested} Waiting {@waiting} Batches {@batches} Messages {@messages}",
                cycles, ingested, waiting, submittedBatches.StatsDescending, submittedMessages.StatsDescending)
            ingested <- 0; cycles <- 0; submittedBatches.Clear(); submittedMessages.Clear()
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats ()
        // Loop, submitting 0 or 1 item per partition per iteration to ensure
        // - each partition has a controlled maximum number of entrants in the scheduler queue
        // - a fair ordering of batch submissions
        let propagate () =
            let mutable more = true
            while more do
                let mutable worked = false
                for KeyValue(_,pq) in buffer do
                    if pq.queue.Count <> 0 then
                        if pq.submissions.Wait(0) then
                            worked <- true
                            let batch = pq.queue.Dequeue()
                            let onCompletion' () =
                                batch.onCompletion()
                                pq.submissions.Release() |> ignore
                            submit { batch with onCompletion = onCompletion' }
                            submittedBatches.Ingest(batch.partitionIndex)
                            submittedMessages.Ingest(batch.partitionIndex,batch.messages.LongLength)
                more <- worked
        /// Take one timeslice worh of ingestion and add to relevant partition queues
        /// When ingested, we allow one propagation submission per partition
        let ingest (partitionBatches : Scheduling.Batch<'M>[]) =
            for x in partitionBatches do
                match buffer.TryGetValue x.partitionIndex with
                | false, _ -> buffer.[x.partitionIndex] <- PartitionQueue<'M>.Create(maxSubmitsPerPartition,x)
                | true, pq -> pq.Append(x)
            propagate()
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable items = Unchecked.defaultof<_>
                if incoming.TryTake(&items, pumpIntervalMs) then
                    ingest items
                    while incoming.TryTake(&items) do
                        ingest items
                maybeLogStats () }
        member __.Submit(items : Scheduling.Batch<'M>[]) =
            Interlocked.Increment(&ingested) |> ignore
            incoming.Add items

module Ingestion =

    type InFlightMessageCounter(log: ILogger, minInFlightBytes : int64, maxInFlightBytes : int64) =
        do  if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L
        member __.InFlightMb = float inFlightBytes / 1024. / 1024.
        member __.Delta(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore
        member __.IsOverLimitNow() = Volatile.Read(&inFlightBytes) > maxInFlightBytes
        member __.AwaitThreshold() =
            log.Warning("Consumer reached in-flight message threshold, breaking off polling, bytes={max}", inFlightBytes)
            while __.IsOverLimitNow() do
                Thread.Sleep 2
            log.Information "Consumer resuming polling"

    /// Retains the messages we've accumulated for a given Partition
    [<NoComparison>]
    type PartitionSpan<'M> =
        {   mutable reservation : int64 // accumulate reserved in flight bytes so we can reverse the reservation when it completes
            mutable highWaterMark : ConsumeResult<string,string> // hang on to it so we can generate a checkpointing lambda
            messages : ResizeArray<'M> }
        member __.Append(sz, message, mapMessage) =
            __.highWaterMark <- message 
            __.reservation <- __.reservation + sz // size we need to unreserve upon completion
            __.messages.Add(mapMessage message)
        static member Create(sz,message,mapMessage) =
            let x = { reservation = 0L; highWaterMark = null; messages = ResizeArray(256) }
            x.Append(sz,message,mapMessage)
            x

    /// Continuously polls across the assigned partitions, building spans; periodically, `submit`s accummulated messages as checkointable Batches
    /// Pauses if in-flight threshold is breached
    type Engine<'M>
        (   log : ILogger, counter : InFlightMessageCounter, consumer : IConsumer<_,_>,
            mapMessage : ConsumeResult<_,_> -> 'M,
            emit : Scheduling.Batch<'M>[] -> unit,
            emitInterval, statsInterval) =
        let acc = Dictionary()
        let remainingIngestionWindow = intervalTimer emitInterval
        let mutable intervalMsgs, intervalChars, totalMessages, totalChars = 0L, 0L, 0L, 0L
        let dumpStats () =
            totalMessages <- totalMessages + intervalMsgs; totalChars <- totalChars + intervalChars
            log.Information("Ingested {msgs:n0} messages, {chars:n0} chars In-flight ~{inflightMb:n1}MB Total {totalMessages:n0} messages {totalChars:n0} chars",
                intervalMsgs, intervalChars, counter.InFlightMb, totalMessages, totalChars)
            intervalMsgs <- 0L; intervalChars <- 0L
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () -> if due () then dumpStats ()
        let ingest message =
            let sz = approximateMessageBytes message
            counter.Delta(+sz) // counterbalanced by Delta(-) in checkpoint(), below
            intervalMsgs <- intervalMsgs + 1L
            intervalChars <- intervalChars + int64 (message.Key.Length + message.Value.Length)
            let partitionId = let p = message.Partition in p.Value
            match acc.TryGetValue partitionId with
            | false, _ -> acc.[partitionId] <- PartitionSpan<'M>.Create(sz,message,mapMessage)
            | true, span -> span.Append(sz,message,mapMessage)
        let submit () =
            match acc.Count with
            | 0 -> ()
            | partitionsWithMessagesThisInterval ->
                let tmp = ResizeArray<Scheduling.Batch<'M>>(partitionsWithMessagesThisInterval)
                for KeyValue(partitionIndex,span) in acc do
                    let checkpoint () =
                        counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, above
                        try consumer.StoreOffset(span.highWaterMark)
                        with e -> log.Error(e, "Consuming... storing offsets failed")
                    tmp.Add { partitionIndex = partitionIndex; onCompletion = checkpoint; messages = span.messages.ToArray() }
                acc.Clear()
                emit <| tmp.ToArray()
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            use _ = consumer // we'll dispose it at the end
            try while not ct.IsCancellationRequested do
                    match counter.IsOverLimitNow(), remainingIngestionWindow () with
                    | true, _ ->
                        submit()
                        maybeLogStats()
                        counter.AwaitThreshold()
                    | false, None ->
                        submit()
                        maybeLogStats()
                    | false, Some intervalRemainder ->
                        try match consumer.Consume(intervalRemainder) with
                            | null -> ()
                            | message -> ingest message
                        with| :? System.OperationCanceledException -> log.Warning("Consuming... cancelled")
                            | :? ConsumeException as e -> log.Warning(e, "Consuming... exception")
                            
            finally
                submit () // We don't want to leak our reservations against the counter and want to pass of messages we ingested
                dumpStats () // Unconditional logging when completing
                consumer.Close() (* Orderly Close() before Dispose() is critical *) }

/// Consumption pipeline that attempts to maximize concurrency of `handle` invocations (up to `dop` concurrently).
/// Consumes according to the `config` supplied to `Start`, until `Stop()` is requested or `handle` yields a fault.
/// Conclusion of processing can be awaited by via `AwaitCompletion()`.
type StreamingConsumer private (log : ILogger, inner : IConsumer<string, string>, task : Task<unit>, cts : CancellationTokenSource) =

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig,
            mapResult : (ConsumeResult<string,string> -> 'M),
            handle : ('M -> Async<Choice<unit,exn>>),
            dop,
            ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)

        let dispatcher = Scheduling.Dispatcher dop
        let scheduler = Scheduling.Engine<'M>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = new Ingestion.InFlightMessageCounter(limiterLog, config.buffering.minInFlightBytes, config.buffering.maxInFlightBytes)
        let consumer = ConsumerBuilder.WithLogging(log, config) // teardown is managed by ingester.Pump()
        let submitter = Submission.Engine(log, 5, 10, scheduler.Submit, statsInterval)
        let ingester = Ingestion.Engine<'M>(log, limiter, consumer, mapResult, submitter.Submit, emitInterval = config.buffering.maxBatchDelay, statsInterval = statsInterval)
        let run (name : string) computation = async {
            try do! computation
                log.Information("Exiting {name}", name)
            with e -> log.Fatal(e, "Abend from {name}", name) }
        let cts = new CancellationTokenSource()
        let machine = async {
            let! ct = Async.CancellationToken
            use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
            let tcs = new TaskCompletionSource<unit>()
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            let! _ = Async.StartChild <| run "dispatcher" (dispatcher.Pump())
            let! _ = Async.StartChild <| run "scheduler" (scheduler.Pump(cts, tcs))
            let! _ = Async.StartChild <| run "submitter" (submitter.Pump())
            let! _ = Async.StartChild <| run "ingester" (ingester.Pump())
            // await for handler faults or external cancellation
            do! Async.AwaitTaskCorrect tcs.Task
        }
        let task = Async.StartAsTask(machine, cancellationToken = cts.Token)
        new StreamingConsumer(log, consumer, task, cts)

    interface IDisposable with member __.Dispose() = __.Stop()

    member __.Inner = inner
    /// Inspects current status of processing task
    member __.Status = task.Status

    /// Request cancellation of processing
    member __.Stop() =  
        log.Information("Consuming ... Stopping {name}", inner.Name)
        cts.Cancel();  

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task