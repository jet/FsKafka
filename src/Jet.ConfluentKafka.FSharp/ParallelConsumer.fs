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

    /// Gathers stats relating to how many items of a given partition have been observed
    type PartitionStats() =
        let partitions = Dictionary<int,int64>()
        member __.Record(partitionId, ?weight) = 
            let weight = defaultArg weight 1L
            match partitions.TryGetValue partitionId with
            | true, catCount -> partitions.[partitionId] <- catCount + weight
            | false, _ -> partitions.[partitionId] <- weight
        member __.Clear() = partitions.Clear()
        member __.StatsDescending = partitions |> Seq.sortBy (fun x -> -x.Value) |> Seq.map (|KeyValue|)

    /// Maintains a Stopwatch such that invoking will yield true at intervals defined by `period`
    let intervalCheck (period : TimeSpan) =
        let timer, max = Stopwatch.StartNew(), int64 period.TotalMilliseconds
        fun () ->
            let due = timer.ElapsedMilliseconds > max
            if due then timer.Restart()
            due

    /// Maintains a Stopwatch used to drive a periodic loop, computing the remaining portion of the period per invocation
    /// - `Some remainder` if the interval has time remaining
    /// - `None` if the interval has expired (and triggers restarting the timer)
    let intervalTimer (period : TimeSpan) =
        let timer = Stopwatch.StartNew()
        fun () ->
            match period - timer.Elapsed with
            | remainder when remainder.Ticks > 0L -> Some remainder
            | _ -> timer.Restart(); None

    /// Can't figure out a cleaner way to shim it :(
    let tryPeek (x : Queue<_>) = if x.Count = 0 then None else Some (x.Peek())

/// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
[<NoComparison; NoEquality>]
type SubmissionBatch<'M> = { partitionId : int; onCompletion: unit -> unit; messages: 'M [] }

/// Deals with dispatch and result handling, triggering completion callbacks as batches reach completed state
module ParallelScheduling =

    /// Single instance per system; coordinates the dispatching of work, subject to the maxDop concurrent processors constraint
    /// Semaphore is allocated on queueing, deallocated on completion of the processing
    type Dispatcher(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag's thread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let dop = new SemaphoreSlim(maxDop)
        /// Attempt to dispatch the supplied task - returns false if processing is presently running at full capacity
        member __.TryAdd task =
            if dop.Wait 0 then work.Add task; true
            else false
        /// Loop that continuously drains the work queue
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            for workItem in work.GetConsumingEnumerable ct do
                Async.Start(async {
                try do! workItem
                // Release the capacity on conclusion of the processing (exceptions should not pass to this level but the correctness here is critical)
                finally dop.Release() |> ignore }) }

    /// Batch of work as passed from the Submitter to the Scheduler comprising messages with their associated checkpointing/completion callback
    [<NoComparison; NoEquality>]
    type Batch<'M> = { partitionId : int; messages: 'M []; onCompletion: unit -> unit }

    /// Thread-safe/lock-free batch-level processing state
    /// - referenced [indirectly, see `mkDispatcher`] among all task invocations for a given batch
    /// - scheduler loop continuously inspects oldest active instance per partition in order to infer attainment of terminal (completed or faulted) state
    [<NoComparison; NoEquality>]
    type WipBatch<'M> =
        {   mutable elapsedMs : int64 // accumulated processing time for stats
            mutable remaining : int // number of outstanding completions; 0 => batch is eligible for completion
            mutable faults : ConcurrentStack<exn> // exceptions, order is not relevant and use is infrequent hence ConcurrentStack
            batch: Batch<'M> }
        member private __.RecordOk(duration : TimeSpan) =
            // need to record stats first as remaining = 0 is used as completion gate
            Interlocked.Add(&__.elapsedMs, int64 duration.TotalMilliseconds + 1L) |> ignore
            Interlocked.Decrement(&__.remaining) |> ignore
        member private __.RecordExn(_duration, exn) =
            __.faults.Push exn
        /// Prepares an initial set of shared state for a batch of tasks, together with the Async<unit> computations that will feed their results into it
        static member Create(batch : Batch<'M>, handle) : WipBatch<'M> * seq<Async<unit>> =
            let x = { elapsedMs = 0L; remaining = batch.messages.Length; faults = ConcurrentStack(); batch = batch }
            x, seq {
                for item in batch.messages -> async {
                    let sw = Stopwatch.StartNew()
                    try let! res = handle item
                        let elapsed = sw.Elapsed
                        match res with
                        | Choice1Of2 () -> x.RecordOk elapsed
                        | Choice2Of2 exn -> x.RecordExn(elapsed, exn)
                    // This exception guard _should_ technically not be necessary per the interface contract, but cannot risk an orphaned batch
                    with exn -> x.RecordExn(sw.Elapsed, exn) } }
    /// Infers whether a WipBatch is in a terminal state
    let (|Busy|Completed|Faulted|) = function
        | { remaining = 0; elapsedMs = ms } -> Completed (TimeSpan.FromMilliseconds <| float ms)
        | { faults = f } when not f.IsEmpty -> Faulted (f.ToArray())
        | _ -> Busy

    /// Continuously coordinates the propagation of incoming requests and mapping that to completed batches
    /// - replenishing the Dispatcher 
    /// - determining when WipBatches attain terminal state in order to triggering completion callbacks at the earliest possible opportunity
    /// - triggering abend of the processing should any dispatched tasks start to fault
    type PartitionedSchedulingEngine<'M>(log : ILogger, handle, tryDispatch : (Async<unit>) -> bool, statsInterval, ?logExternalStats) =
        // Submitters dictate batch commencement order by supply batches in a fair order; should never be empty if there is work in the system
        let incoming = ConcurrentQueue<Batch<'M>>()
        // Prepared work items ready to feed to Dispatcher (only created on demand in order to ensure we maximize overall progress and fairness)
        let waiting = Queue<Async<unit>>(1024)
        // Index of batches that have yet to attain terminal state (can be >1 per partition)
        let active = Dictionary<int(*partitionId*),Queue<WipBatch<'M>>>()
        (* accumulators for periodically emitted statistics info *)
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
                seq { for KeyValue(pid,q) in active -> pid, q |> Seq.sumBy (fun x -> x.remaining) }
                |> Seq.filter (fun (_,snd) -> snd <> 0)
                |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Partitions Active items {@active} Started batches {@startedBatches} items {@startedItems} Completed batches {@completedBatches} items {@completedItems}",
                active, startedB, startedI, completedB, completedI)
            cycles <- 0; processingDuration <- TimeSpan.Zero; startedBatches.Clear(); completedBatches.Clear(); startedItems.Clear(); completedItems.Clear()
            logExternalStats |> Option.iter (fun f -> f log) // doing this in here allows stats intervals to be aligned with that of the scheduler engine
        let maybeLogStats : unit -> bool =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats (); true else false
        /// Inspects the oldest in-flight batch per partition to determine if it's reached a terminal state; if it has, remove and trigger completion callback
        let drainCompleted abend =
            let mutable more, worked = true, false
            while more do
                more <- false
                for queue in active.Values do
                    match tryPeek queue with
                    | None // empty
                    | Some Busy -> () // still working
                    | Some (Faulted exns) -> // outer layers will react to this by tearing us down
                        abend (AggregateException(exns))
                    | Some (Completed batchProcessingDuration) -> // call completion function asap
                        let partitionId, markCompleted, itemCount =
                            let { batch = { partitionId = p; onCompletion = f; messages = msgs } } = queue.Dequeue()
                            p, f, msgs.LongLength
                        completedBatches.Record partitionId
                        completedItems.Record(partitionId, itemCount)
                        processingDuration <- processingDuration.Add batchProcessingDuration
                        markCompleted ()
                        worked <- true
                        more <- true // vote for another iteration as the next one could already be complete too. Not looping inline/immediately to give others partitions equal credit
            worked
        /// Unpacks a new batch from the queue; each item goes through the `waiting` queue as the loop will continue to next iteration if dispatcher is full
        let tryPrepareNext () =
            match incoming.TryDequeue() with
            | false, _ -> false
            | true, ({ partitionId = pid; messages = msgs} as batch) ->
                startedBatches.Record(pid)
                startedItems.Record(pid, msgs.LongLength)
                let wipBatch, runners = WipBatch.Create(batch, handle)
                runners |> Seq.iter waiting.Enqueue
                match active.TryGetValue pid with
                | false, _ -> let q = Queue(1024) in active.[pid] <- q; q.Enqueue wipBatch
                | true, q -> q.Enqueue wipBatch
                true
        /// Tops up the current work in progress
        let reprovisionDispatcher () =
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

        /// Main pumping loop; `abend` is a callback triggered by a faulted task which the outer controler can use to shut down the processing
        member __.Pump abend = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let hadResults = drainCompleted abend
                let queuedWork = reprovisionDispatcher ()
                let loggedStats = maybeLogStats ()
                if not hadResults && not queuedWork && not loggedStats then
                    Thread.Sleep 1 } // not Async.Sleep, we like this context and/or cache state if nobody else needs it

        /// Feeds a batch of work into the queue; the caller is expected to ensure sumbissions are timely to avoid starvation, but throttled to ensure fair ordering
        member __.Submit(batches : Batch<'M>) =
            incoming.Enqueue batches

/// Holds batches from the Ingestion pipe, feeding them continuously to the scheduler in an appropriate order
module Submission =
    /// Holds the queue for a given partition, together with a semaphore we use to ensure the number of in-flight batches per partition is constrained
    [<NoComparison>]
    type PartitionQueue<'B> = { submissions: SemaphoreSlim; queue : Queue<'B> } with
        member __.Append(batch) = __.queue.Enqueue batch
        static member Create(maxSubmits) = { submissions = new SemaphoreSlim(maxSubmits); queue = Queue(maxSubmits * 2) } 

    /// Holds the stream of incoming batches, grouping by partition
    /// Manages the submission of batches into the Scheduler in a fair manner
    type SubmissionEngine<'M,'B>
        (   log : ILogger, maxSubmitsPerPartition, mapBatch: (unit -> unit) -> SubmissionBatch<'M> -> 'B, submitBatch : 'B -> int, statsInterval, ?pumpInterval : TimeSpan,
            ?tryCompactQueue) =
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let incoming = new BlockingCollection<SubmissionBatch<'M>[]>(ConcurrentQueue())
        let buffer = Dictionary<int,PartitionQueue<'B>>()
        let mutable cycles, ingested, compacted = 0, 0, 0
        let submittedBatches,submittedMessages = PartitionStats(), PartitionStats()
        let dumpStats () =
            let waiting = seq { for x in buffer do if x.Value.queue.Count <> 0 then yield x.Key, x.Value.queue.Count } |> Seq.sortBy (fun (_,snd) -> -snd)
            log.Information("Submitter {cycles} cycles {ingested} accepted {compactions} compactions Holding {@waiting}", cycles, ingested, compacted, waiting)
            log.Information(" Submitted Batches {@batches} Messages {@messages}", submittedBatches.StatsDescending, submittedMessages.StatsDescending)
            ingested <- 0; compacted <- 0; cycles <- 0; submittedBatches.Clear(); submittedMessages.Clear()
        let maybeLogStats =
            let due = intervalCheck statsInterval
            fun () ->
                cycles <- cycles + 1
                if due () then dumpStats ()
        // Loop, submitting 0 or 1 item per partition per iteration to ensure
        // - each partition has a controlled maximum number of entrants in the scheduler queue
        // - a fair ordering of batch submissions
        let propagate () =
            let mutable more, worked = true, false
            while more do
                more <- false
                for KeyValue(pi,pq) in buffer do
                    if pq.queue.Count <> 0 then
                        if pq.submissions.Wait(0) then
                            worked <- true
                            more <- true
                            let count = submitBatch <| pq.queue.Dequeue()
                            submittedBatches.Record(pi)
                            submittedMessages.Record(pi, int64 count)
            worked
        /// Take one timeslice worth of ingestion and add to relevant partition queues
        /// When ingested, we allow one propagation submission per partition
        let ingest (partitionBatches : SubmissionBatch<'M>[]) =
            for { partitionId = pid } as batch in partitionBatches do
                let pq =
                    match buffer.TryGetValue pid with
                    | false, _ -> let t = PartitionQueue<_>.Create(maxSubmitsPerPartition) in buffer.[pid] <- t; t
                    | true, pq -> pq
                let mapped = mapBatch (fun () -> pq.submissions.Release() |> ignore) batch
                pq.Append(mapped)
            propagate()
        /// We use timeslices where we're we've fully provisioned the scheduler to index any waiting Batches
        let compact f =
            let mutable worked = false
            for KeyValue(_,pq) in buffer do
                if f pq.queue then
                    worked <- true
            if worked then compacted <- compacted + 1; true
            else false

        /// Processing loop, continuously splitting `Submit`ted items into per-partition queues and ensuring enough items are provided to the Scheduler
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                let mutable items = Unchecked.defaultof<_>
                let mutable propagated = false
                if incoming.TryTake(&items, pumpInterval) then
                    propagated <- ingest items
                    while incoming.TryTake(&items) do
                        if ingest items then propagated <- true
                else propagated <- propagate()
                match propagated, tryCompactQueue with
                | false, None -> Thread.Sleep 2
                | false, Some f when not (compact f) -> Thread.Sleep 2
                | _ -> ()

                maybeLogStats () }

        /// Supplies a set of Batches for holding and forwarding to scheduler at the right time
        member __.Ingest(items : SubmissionBatch<'M>[]) =
            Interlocked.Increment(&ingested) |> ignore
            incoming.Add items

/// Manages efficiently and continuously reading from the Confluent.Kafka consumer, offloading the pushing of those batches onward to the Submitter
/// Responsible for ensuring we over-read, which would cause the rdkafka buffers to overload the system in terms of memory usage
[<AutoOpen>]
module KafkaIngestion =

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
            x.Append(sz, message, mapMessage)
            x

    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    /// Continuously polls across the assigned partitions, building spans; periodically (at intervals of `emitInterval`), `submit`s accummulated messages as
    ///   checkpointable Batches
    /// Pauses if in-flight upper threshold is breached until such time as it drops below that the lower limit
    type KafkaIngestionEngine<'M>
        (   log : ILogger, counter : Core.InFlightMessageCounter, consumer : IConsumer<_,_>, mapMessage : ConsumeResult<_,_> -> 'M, emit : SubmissionBatch<'M>[] -> unit,
            emitInterval, statsInterval) =
        let acc = Dictionary()
        let remainingIngestionWindow = intervalTimer emitInterval
        let mutable intervalMsgs, intervalChars, totalMessages, totalChars = 0L, 0L, 0L, 0L
        let dumpStats () =
            totalMessages <- totalMessages + intervalMsgs; totalChars <- totalChars + intervalChars
            log.Information("Ingested {msgs:n0}m, {chars:n0}c In-flight ~{inflightMb:n1}MB Σ {totalMessages:n0} messages, {totalChars:n0} chars",
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
                let tmp = ResizeArray<SubmissionBatch<'M>>(partitionsWithMessagesThisInterval)
                for KeyValue(partitionIndex,span) in acc do
                    let checkpoint () =
                        counter.Delta(-span.reservation) // counterbalance Delta(+) per ingest, above
                        try consumer.StoreOffset(span.highWaterMark)
                        with e -> log.Error(e, "Consuming... storing offsets failed")
                    tmp.Add { partitionId = partitionIndex; onCompletion = checkpoint; messages = span.messages.ToArray() }
                acc.Clear()
                emit <| tmp.ToArray()
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            use _ = consumer // Dispose it at the end (NB but one has to Close first or risk AccessViolations etc)
            try while not ct.IsCancellationRequested do
                    match counter.IsOverLimitNow(), remainingIngestionWindow () with
                    | true, _ ->
                        let busyWork () =
                            submit()
                            maybeLogStats()
                            Thread.Sleep 1 
                        counter.AwaitThreshold busyWork
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
type PipelinedConsumer private (inner : IConsumer<string, string>, task : Task<unit>, triggerStop) =

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Provides access to the Confluent.Kafka interface directly
    member __.Inner = inner
    /// Inspects current status of processing task
    member __.Status = task.Status
    /// After AwaitCompletion, can be used to infer whether exit was clean
    member __.RanToCompletion = task.Status = TaskStatus.RanToCompletion 

    /// Request cancellation of processing
    member __.Stop() = triggerStop ()

    /// Asynchronously awaits until consumer stops or a `handle` invocation yields a fault
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start(log : ILogger, config : KafkaConsumerConfig, mapResult, submit, pumpSubmitter, pumpScheduler, pumpDispatcher, statsInterval) =
        log.Information("Consuming... {broker} {topics} {groupId} autoOffsetReset {autoOffsetReset} fetchMaxBytes={fetchMaxB} maxInFlight={maxInFlightGB:n1}GB maxBatchDelay={maxBatchDelay}s",
            config.Inner.BootstrapServers, config.Topics, config.Inner.GroupId, (let x = config.Inner.AutoOffsetReset in x.Value), config.Inner.FetchMaxBytes,
            float config.Buffering.maxInFlightBytes / 1024. / 1024. / 1024., (let t = config.Buffering.maxBatchDelay in t.TotalSeconds))
        let limiterLog = log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, Core.Constants.messageCounterSourceContext)
        let limiter = new Core.InFlightMessageCounter(limiterLog, config.Buffering.minInFlightBytes, config.Buffering.maxInFlightBytes)
        let consumer = ConsumerBuilder.WithLogging(log, config) // teardown is managed by ingester.Pump()
        let ingester = KafkaIngestionEngine<'M>(log, limiter, consumer, mapResult, submit, emitInterval = config.Buffering.maxBatchDelay, statsInterval = statsInterval)
        let cts = new CancellationTokenSource()
        let ct = cts.Token
        let tcs = new TaskCompletionSource<unit>()
        let triggerStop () =
            log.Information("Consuming ... Stopping {name}", consumer.Name)
            cts.Cancel();  
        let start name f =
            let wrap (name : string) computation = async {
                try do! computation
                    log.Information("Exiting pipeline component {name}", name)
                with e ->
                    log.Fatal(e, "Abend from pipeline component {name}", name)
                    triggerStop() }
            Async.Start(wrap name f, ct)
        // if scheduler encounters a faulted handler, we propagate that as the consumer's Result
        let abend (exns : AggregateException) =
            if tcs.TrySetException(exns) then log.Warning(exns, "Cancelling processing due to {count} faulted handlers", exns.InnerExceptions.Count)
            else log.Information("Failed setting {count} exceptions", exns.InnerExceptions.Count)
            // NB cancel needs to be after TSE or the Register(TSE) will win
            cts.Cancel()

        let machine = async {
            // external cancellation should yield a success result
            use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)
            start "dispatcher" <| pumpDispatcher
            // ... fault results from dispatched tasks result in the `machine` concluding with an exception
            start "scheduler" <| pumpScheduler abend
            start "submitter" <| pumpSubmitter
            start "ingester" <| ingester.Pump()

            // await for either handler-driven abend or external cancellation via Stop()
            do! Async.AwaitTaskCorrect tcs.Task
        }
        let task = Async.StartAsTask machine
        new PipelinedConsumer(consumer, task, triggerStop)

[<AbstractClass; Sealed>]
type ParallelConsumer private () =
    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start<'M>
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, mapResult : (ConsumeResult<string,string> -> 'M), handle : ('M -> Async<Choice<unit,exn>>),
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 5.)
        let pumpInterval = defaultArg pumpInterval (TimeSpan.FromMilliseconds 5.)
        let maxSubmissionsPerPartition = defaultArg maxSubmissionsPerPartition 5

        let dispatcher = ParallelScheduling.Dispatcher maxDop
        let scheduler = ParallelScheduling.PartitionedSchedulingEngine<'M>(log, handle, dispatcher.TryAdd, statsInterval, ?logExternalStats=logExternalStats)
        let mapBatch onCompletion (x : SubmissionBatch<_>) : ParallelScheduling.Batch<'M> =
            let onCompletion' () = x.onCompletion(); onCompletion()
            { partitionId = x.partitionId; messages = x.messages; onCompletion = onCompletion'; } 
        let submitBatch (x : ParallelScheduling.Batch<_>) : int =
            scheduler.Submit x
            x.messages.Length
        let submitter = Submission.SubmissionEngine(log, maxSubmissionsPerPartition, mapBatch, submitBatch, statsInterval, pumpInterval)
        PipelinedConsumer.Start(log, config, mapResult, submitter.Ingest, submitter.Pump(), scheduler.Pump, dispatcher.Pump(), statsInterval)

    /// Builds a processing pipeline per the `config` running up to `dop` instances of `handle` concurrently to maximize global throughput across partitions.
    /// Processor pumps until `handle` yields a `Choice2Of2` or `Stop()` is requested.
    static member Start
        (   log : ILogger, config : KafkaConsumerConfig, maxDop, handle : KeyValuePair<string,string> -> Async<unit>,
            ?maxSubmissionsPerPartition, ?pumpInterval, ?statsInterval, ?logExternalStats) =
        let mapConsumeResult (x : ConsumeResult<string,string>) = KeyValuePair(x.Key, x.Value)
        ParallelConsumer.Start<KeyValuePair<string,string>>(log, config, maxDop, mapConsumeResult, handle >> Async.Catch,
            ?maxSubmissionsPerPartition=maxSubmissionsPerPartition, ?pumpInterval=pumpInterval, ?statsInterval=statsInterval, ?logExternalStats=logExternalStats)