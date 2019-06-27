module Jet.ConfluentKafka.FSharp.Integration.MonitorIntegration

open Jet.ConfluentKafka.FSharp
open System
open Xunit
open Confluent.Kafka
open System.Threading
open Swensen.Unquote

let mkGuid () = let g = System.Guid.NewGuid() in g.ToString("N")
let mkMonitor log = KafkaMonitor(log, TimeSpan.FromSeconds 3., windowSize = 5)
let mkProducer log broker topic =
    // Needs to be random to fill al partitions
    let config = KafkaProducerConfig.Create("tiger", broker, Acks.Leader, partitioner = Partitioner.Random)
    KafkaProducer.Create(log, config, topic)
let createConsumerConfig broker topic groupId =
    KafkaConsumerConfig.Create("tiger", broker, [topic], groupId, maxBatchSize = 1)
let startConsumerFromConfig log config handler =
    BatchedConsumer.Start(log, config, handler)
let startConsumer log broker topic groupId handler =
    let config = createConsumerConfig broker topic groupId
    BatchedConsumer.Start(log, config, handler)
let producerOnePerSecondLoop (producer : KafkaProducer) =
    let rec loop () = async {
        let! _ = producer.ProduceAsync("a","1")
        do! Async.Sleep 1000
        return! loop () }
    loop ()
let onlyConsumeFirstBatchHandler =
    let observedPartitions = System.Collections.Concurrent.ConcurrentDictionary()
    fun (items : ConsumeResult<string,string>[]) -> async {
        // make first handle succeed to ensure consumer has offsets
        let partitionId = let p = items.[0].Partition in p.Value
        if not <| observedPartitions.TryAdd(partitionId,()) then do! Async.Sleep Int32.MaxValue }

type T1(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<Fact>]
    let ``Monitor should detect stalled consumer`` () = async {
        let topic, group = mkGuid (), mkGuid () // dev kafka topics are created and truncated automatically
        let producer = mkProducer log broker topic
        let! _producerActivity = Async.StartChild <| producerOnePerSecondLoop producer

        let mutable errorObserved = false
        let observeErrorsMonitorHandler(_topic,states : (int * PartitionResult) list) =
            errorObserved <- errorObserved
                || states |> List.exists (function _,PartitionResult.ErrorPartitionStalled _ -> true | _ -> false)

        // start stalling consumer
        use consumer = startConsumer log broker topic group onlyConsumeFirstBatchHandler
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(observeErrorsMonitorHandler)
        do! monitor.StartAsChild(consumer.Inner, group)
        while not <| Volatile.Read(&errorObserved) do
            do! Async.Sleep 1000 }

type T2(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<Fact>]
    let ``Monitor should continue checking progress after rebalance`` () = async {
        let topic, group = mkGuid (), mkGuid () // dev kafka topics are created and truncated automatically
        let producer = mkProducer log broker topic
        let mutable progressChecked, numPartitions = false, 0

        let partitionsObserver(_topic, errors : (int * PartitionResult) list) =
            progressChecked <- true
            numPartitions <- errors.Length

        let! _producerActivity = Async.StartChild <| producerOnePerSecondLoop producer
        
        use consumerOne = startConsumer log broker topic group onlyConsumeFirstBatchHandler 
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(partitionsObserver)
        do! monitor.StartAsChild(consumerOne.Inner, group)
        // first consumer is only member of group, should have all partitions
        while 4 <> Volatile.Read(&numPartitions) do
            do! Async.Sleep 1000

        4 =! numPartitions

        // create second consumer and join group to trigger rebalance
        use _consumerTwo = startConsumer log broker topic group onlyConsumeFirstBatchHandler
        progressChecked <- false

        // make sure the progress was checked after rebalance
        while 2 <> Volatile.Read(&numPartitions) do
            do! Async.Sleep 1000
        
        // with second consumer in group, first consumer should have half of the partitions
        2 =! numPartitions
    }

type T3(testOutputHelper) =
    let log, broker = createLogger (TestOutputAdapter testOutputHelper), getTestBroker ()

    [<Fact>]

    let ``Monitor should not join consumer group`` () = async {
        let topic, group = mkGuid (), mkGuid () // dev kafka topics are created and truncated automatically
        let noopObserver _ = ()
        let config = createConsumerConfig broker topic group
        use consumer = startConsumerFromConfig log config onlyConsumeFirstBatchHandler
        let monitor = mkMonitor log
        use _ = monitor.OnStatus.Subscribe(noopObserver)
        do! monitor.StartAsChild(consumer.Inner, group)
        while consumer.Inner.Assignment.Count = 0 do
            do! Async.Sleep 1000

        let acc = AdminClientConfig(config.Inner)
        let ac = AdminClientBuilder(acc).Build()

        // should be one member in group
        1 =! ac.ListGroup(group, TimeSpan.FromSeconds 30.).Members.Count
        // consumer should have all 4 partitions assigned to it
        4 =! consumer.Inner.Assignment.Count
    }