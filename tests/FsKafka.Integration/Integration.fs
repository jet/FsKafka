namespace FsKafka.Integration

open Confluent.Kafka
open FsKafka
open Newtonsoft.Json
open Serilog
open Swensen.Unquote
open System
open System.Collections.Concurrent
open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open Xunit

module Config =
    let validateBrokerUri (broker : Uri) =
        if not broker.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty broker.Authority then
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string broker, "^\S+:[0-9]+$") then string broker
            else invalidArg "broker" "should be of 'host:port' format"

        else broker.Authority

[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module Helpers =

    // Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
    // NB VS does not surface these atm, but other test runners / test reports do
    type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
        let writeSerilogEvent logEvent =
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer);
            writer |> string |> testOutput.WriteLine
            writer |> string |> System.Diagnostics.Debug.WriteLine
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

    let createLogger sink =
        LoggerConfiguration()
            .Destructure.FSharpTypes()
            .WriteTo.Sink(sink)
            .WriteTo.Seq("http://localhost:5341")
            .CreateLogger()

    let getTestBroker() = 
        match Environment.GetEnvironmentVariable "TEST_KAFKA_BROKER" with
        | x when String.IsNullOrEmpty x -> invalidOp "missing environment variable 'TEST_KAFKA_BROKER'"
        | x -> Uri x |> Config.validateBrokerUri

    let newId () = let g = System.Guid.NewGuid() in g.ToString("N")

    type Async with
        static member ParallelThrottled degreeOfParallelism jobs =
            let s = new SemaphoreSlim(degreeOfParallelism)
            jobs
            |> Seq.map (fun j -> async {
                let! ct = Async.CancellationToken
                do! s.WaitAsync ct |> Async.AwaitTask
                try return! j
                finally s.Release() |> ignore })
            |> Async.Parallel

    type BatchedConsumer with
        member c.StopAfter(delay : TimeSpan) =
            Task.Delay(delay).ContinueWith(fun (_:Task) -> c.Stop()) |> ignore

    type TestMessage = { producerId : int ; messageId : int }
    [<NoComparison; NoEquality>]
    type ConsumedTestMessage = { consumerId : int ; result : ConsumeResult<string,string> ; payload : TestMessage }
    type ConsumerCallback = BatchedConsumer -> ConsumedTestMessage [] -> Async<unit>

    let runProducers log broker (topic : string) (numProducers : int) (messagesPerProducer : int) = async {
        let runProducer (producerId : int) = async {
            let cfg = KafkaProducerConfig.Create("panther", broker, Acks.Leader, Batching.Custom (TimeSpan.FromMilliseconds 100., 10_000))
            use producer = BatchedProducer.Create(log, cfg, topic)

            let! results =
                [1 .. messagesPerProducer]
                |> Seq.map (fun msgId ->
                    let key = string msgId
                    let value = JsonConvert.SerializeObject { producerId = producerId ; messageId = msgId }
                    key, value)
                |> Seq.chunkBySize 100
                |> Seq.map producer.ProduceBatch
                |> Async.ParallelThrottled 7

            return Array.concat results
        }

        return! Async.Parallel [for i in 1 .. numProducers -> runProducer i]
    }

    let runConsumers log (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            let deserialize result =
                let message = Binding.message result
                { consumerId = consumerId ; result = result ; payload = JsonConvert.DeserializeObject<_> message.Value }

            // need to pass the consumer instance to the handler callback; perform some cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            let partitionHandler batch = handler (getConsumer()) (Array.map deserialize batch)
            use consumer = BatchedConsumer.Start(log, config, partitionHandler)

            consumerCell := Some consumer

            timeout |> Option.iter consumer.StopAfter

            return! consumer.AwaitShutdown()
        }

        return! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }

type FactIfBroker() =
    inherit FactAttribute()
    override __.Skip = if null <> Environment.GetEnvironmentVariable "TEST_KAFKA_BROKER" then null else "Skipping as no TEST_KAFKA_BROKER supplied"

type T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``ConfluentKafka producer-consumer basic roundtrip`` () = async {
        let numProducers = 10
        let numConsumers = 10
        let messagesPerProducer = 1000

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
    
        let consumedBatches = ConcurrentBag<ConsumedTestMessage[]>()
        let consumerCallback (consumer:BatchedConsumer) batch = async {
            do consumedBatches.Add batch
            let distinct = Seq.collect id consumedBatches |> Seq.map (fun x -> x.payload.producerId, x.payload.messageId) |> Seq.distinct
            if Seq.length distinct >= numProducers * messagesPerProducer then
                consumer.Stop()
        }

        // Section: run the test
        let producers = runProducers log broker topic numProducers messagesPerProducer |> Async.Ignore

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, AutoOffsetReset.Earliest, statisticsInterval=TimeSpan.FromSeconds 5.)
        let consumers = runConsumers log config numConsumers None consumerCallback

        let! _ = [ producers ; consumers ] |> Async.Parallel

        // Section: assertion checks
        let ``consumed batches should be non-empty`` =
            consumedBatches
            |> Seq.forall (not << Array.isEmpty)

        test <@ ``consumed batches should be non-empty`` @> // "consumed batches should all be non-empty")

        let ``batches should be grouped by partition`` =
            consumedBatches
            |> Seq.map (fun batch -> batch |> Seq.distinctBy (fun b -> b.result.Partition) |> Seq.length)
            |> Seq.forall (fun numKeys -> numKeys = 1)
        
        test <@ ``batches should be grouped by partition`` @> // "batches should be grouped by partition"

        let allMessages =
            consumedBatches
            |> Seq.concat
            |> Seq.toArray

        let ``all message keys should have expected value`` =
            allMessages |> Array.forall (fun msg ->
                let message = Binding.message msg.result
                int message.Key = msg.payload.messageId)

        test <@ ``all message keys should have expected value`` @> // "all message keys should have expected value"

        let ``grouped messages`` =
            allMessages
            |> Array.groupBy (fun msg -> msg.payload.producerId)
            |> Array.map (fun (_, gp) -> gp |> Array.distinctBy (fun msg -> msg.payload.messageId) |> Array.length)

        test <@ ``grouped messages``
                |> Array.forall (fun gmc -> gmc = messagesPerProducer) @> // "should have consumed all expected messages"
    }

// separated test type to allow the tests to run in parallel
type T2(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``BatchedConsumer should have expected exception semantics in face of handler exception`` () = async {
        let topic, groupId = newId(), newId() // dev kafka topics are created and truncated automatically

        let! _ = runProducers log broker topic 1 10 // populate the topic with a few messages

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, AutoOffsetReset.Earliest)
        
        let! r = Async.Catch <| runConsumers log config 1 None (fun _ _ -> raise <|IndexOutOfRangeException())
        test <@ match r with Choice2Of2 (:? IndexOutOfRangeException) -> true | x -> failwithf "%A" x @>
    }

    let [<FactIfBroker>] ``Given a topic different consumer group ids should be consuming the same message set`` () = async {
        let numMessages = 10

        let topic = newId() // dev kafka topics are created and truncated automatically

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        let messageCount = ref 0
        let groupId1 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId1, AutoOffsetReset.Earliest)
        do! runConsumers log config 1 None 
                (fun c b -> async { if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        test <@ numMessages = !messageCount @>

        let messageCount = ref 0
        let groupId2 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId2, AutoOffsetReset.Earliest)
        do! runConsumers log config 1 None
                (fun c b -> async { if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        test <@ numMessages = !messageCount @>
    }

    let [<FactIfBroker>] ``Spawning a new consumer with same consumer group id should not receive new messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, AutoOffsetReset.Earliest)

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be stored

        test <@ numMessages = !messageCount @>

        // expected to read no messages from the subsequent consumer
        let messageCount = ref 0
        do! runConsumers log config 1 (Some (TimeSpan.FromSeconds 10.)) 
                (fun c b -> async { 
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        test <@ 0 = !messageCount @>
    }

// separated test type to allow the tests to run in parallel
type T3(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    let [<FactIfBroker>] ``Committed offsets should not result in missing messages`` () = async {
        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, AutoOffsetReset.Earliest)

        let! _ = runProducers log broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>

        let! _ = runProducers log broker topic 1 numMessages // produce more messages

        // expected to read 10 messages from the subsequent consumer,
        // this is to verify there are no off-by-one errors in how offsets are committed
        let messageCount = ref 0
        do! runConsumers log config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        test <@ numMessages = !messageCount @>
    }

    let [<FactIfBroker>] ``Consumers should never schedule two batches of the same partition concurrently`` () = async {
        // writes 2000 messages down a topic with a shuffled partition key
        // then attempts to consume the topic, while verifying that per-partition batches
        // are never scheduled for concurrent execution. also checks that batches are
        // monotonic w.r.t. offsets
        let numMessages = 2000
        let maxBatchSize = 5
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, AutoOffsetReset.Earliest, maxBatchSize = maxBatchSize)

        // Produce messages in the topic
        do! runProducers log broker topic 1 numMessages |> Async.Ignore

        let globalMessageCount = ref 0

        let getPartitionOffset = 
            let state = ConcurrentDictionary<int, int64 ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref -1L)

        let getBatchPartitionCount =
            let state = ConcurrentDictionary<int, int ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref 0)

        do! runConsumers log config 1 None
                (fun c b -> async {
                    let partition = Binding.partitionValue b.[0].result.Partition

                    // check batch sizes are bounded by maxBatchSize
                    test <@ b.Length <= maxBatchSize @> // "batch sizes should never exceed maxBatchSize")

                    // check per-partition handlers are serialized
                    let concurrentBatchCell = getBatchPartitionCount partition
                    let concurrentBatches = Interlocked.Increment concurrentBatchCell
                    test <@ 1 = concurrentBatches @> // "partitions should never schedule more than one batch concurrently")

                    // check for message monotonicity
                    let offset = getPartitionOffset partition
                    for msg in b do
                        Assert.True((let o = msg.result.Offset in o.Value) > !offset, "offset for partition should be monotonic")
                        offset := let o = msg.result.Offset in o.Value

                    do! Async.Sleep 100

                    let _ = Interlocked.Decrement concurrentBatchCell

                    if Interlocked.Add(globalMessageCount, b.Length) >= numMessages then c.Stop() })

        test <@ numMessages = !globalMessageCount @>
    }

// separated test type to allow the tests to run in parallel
type T4(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()
    let testValue = String('v', 1024)
    let testKeys = set [for x in 1..2 -> string x]
    
    let produce topic = async {
        let producerCfg = KafkaProducerConfig.Create("panther", broker, Acks.Leader, Batching.Linger (TimeSpan.FromMilliseconds 10.))
        use producer = KafkaProducer.Create(log, producerCfg, topic)
        return! Async.Parallel [for key in testKeys do producer.ProduceAsync(key, testValue) ]
    }

    let consumerCfg topic groupId=
        KafkaConsumerConfig.Create(
            "panther", broker, [topic], groupId, AutoOffsetReset.Earliest,
            maxInFlightBytes=1_000L,
            customize=fun c ->
#if KAFKA0
                ()
#else
                // these properties are not implemented in FsKafka0
                c.MaxPollIntervalMs <- Nullable 10_000 // Default is 5m, needs to exceed SessionTimeoutMs
                c.SessionTimeoutMs <- Nullable 6_000 // Broker default min value is 6000
#endif
        )
            
    let handle (timer : Diagnostics.Stopwatch) (received : ConcurrentQueue<string*int64>) (callCount : int64 ref) messages = async {
        for r in messages do
            let m = Binding.message r
            log.Information("Received {key} at {time}", m.Key, timer.ElapsedMilliseconds)
            received.Enqueue((m.Key, timer.ElapsedMilliseconds))
        match Interlocked.Increment callCount with
        | 1L ->
            // Drive the quiescing period over the MaxPollInterval
            do! Async.Sleep 10_500
        | _ when received.Count < testKeys.Count ->
            ()
        | _ ->
            failwith "Completed"
    }
    
#if !KAFKA0 // TODO if Kafka0 usage remains prevalent, figure out why this hangs ~25% of the time
    let [<FactIfBroker>] ``Without FsKafka.Monitor - BatchedConsumer should have expected quiescing semantics`` () = async {
        let topic, groupId = newId(), newId() // dev kafka topics are created and truncated automatically
        let! _ = produce topic
        
        let timer = System.Diagnostics.Stopwatch.StartNew()
        let received = ConcurrentQueue()
        let callCount = ref 0L
        let consumerCfg = consumerCfg topic groupId
        let handle = handle timer received callCount
        
        let! res = async {
            use consumer = BatchedConsumer.Start(log, consumerCfg, handle)
            consumer.StopAfter (TimeSpan.FromSeconds 20.)
            return! consumer.AwaitShutdown() |> Async.Catch
        }
        
        test <@ match res with Choice2Of2 e when e.Message = "Completed" -> true | _ -> false @>
        test <@ set (Seq.map fst received) = testKeys @>
    }

    let [<FactIfBroker>] ``With FsKafka.Monitor - BatchedConsumer should have expected quiescing semantics`` () = async {
        let topic, groupId = newId(), newId() // dev kafka topics are created and truncated automatically
        let! _ = produce topic
        
        let timer = System.Diagnostics.Stopwatch.StartNew()
        let received = ConcurrentQueue()
        let callCount = ref 0L
        let consumerCfg = consumerCfg topic groupId
        let handle = handle timer received callCount
        
        let! res = async {
            use consumer = BatchedConsumer.Start(log, consumerCfg, handle)
            consumer.StopAfter (TimeSpan.FromSeconds 20.)
            use _ = FsKafka.KafkaMonitor(log).Start(consumer.Inner, consumerCfg.Inner.GroupId)
            return! consumer.AwaitShutdown() |> Async.Catch
        }
        
        test <@ match res with Choice2Of2 e when e.Message = "Completed" -> true | _ -> false @>
        test <@ set (Seq.map fst received) = testKeys @>
    }
#endif
