module Jet.ConfluentKafka.FSharp.Integration.MonitorIntegration

open Jet.ConfluentKafka.FSharp.Monitor
open Serilog
open System
open Xunit
open Newtonsoft.Json

let kafkaConn = "shared.kafka.eastus2.dev.jet.network:9092"
let testGroup = "thor-asgard-test-consumer-group"
let testTopic = "thor-asgard-test-topic"

type ILogger with
    member __.ForContextNamed name = __.ForContext(Serilog.Core.Constants.SourceContextPropertyName, name)

let createTestConsumer () =
    Kafka.Consumer.createDefaultConsumer kafkaConn testTopic testGroup

let createTestProducer () = Kafka.Producer.createDefaultProducer kafkaConn

let createTestMonitorConfig consumer errorHandler =
    {     KafkaMonitorConfig.Default kafkaConn testGroup consumer with
              pollInterval = TimeSpan.FromSeconds 5.
              windowSize = 10
              errorHandler = errorHandler }

[<Fact>]
let ``Monitor should detect stalled consumer`` () = async {
    let producerLog = Log.Logger.ForContextNamed "ProducerLogger"
    let producer = createTestProducer()
    let consumer = createTestConsumer()

    let mutable errorReceived = false

    let errorHandler _ _ (errors : (int * PartitionResult) []) =
        let hasError =
            errors
            |> Array.tryFind (fun (_, error) -> error <> NoError)
            |> Option.isSome

        if hasError then errorReceived <- true

    let monitorConfig = createTestMonitorConfig consumer errorHandler

    let mutable firstHandle = true
    let laggingHandle _  =
        if firstHandle then
            // make first handle succeed to ensure consumer has offsets
            firstHandle <- false
            async { () }
        else
            Async.Sleep(100000000)

    let rec producerLoop () = async {
        let! _ = Kafka.Producer.produceBatch producer testTopic [| JsonConvert.SerializeObject( [| "x" .= 1|], (string 1))|]
        do! Async.Sleep(1000)
        return! producerLoop()
    }

    // start test producer
    producerLoop ()
    |> Async.Start
    
    // start stalled consumer
    Kafka.Consumer.consume consumer laggingHandle 
    |> runWithKafkaMonitor monitorConfig
    |> Async.Start

    do! Async.Sleep(120000)

    Assert.True(errorReceived)
    consumer.ConfluentConsumer.Unsubscribe()
}

[<Fact>]
let ``Monitor should continue checking progress after rebalance`` () = async {
    let producerLog = Log.Logger.ForContextNamed("ProducerLogger")
    let producer = createTestProducer()
    let consumerOne = createTestConsumer()

    let mutable progressChecked = false
    let mutable numPartitions = 0

    let errorHandler _ _ (errors : (int * PartitionResult) []) =
        numPartitions <- errors.Length
        progressChecked <- true

    let monitorConfig = createTestMonitorConfig consumerOne errorHandler

    let mutable firstHandle = true
    let laggingHandle _  =
        if firstHandle then
            // make first handle succeed to ensure consumer has offsets
            firstHandle <- false
            async { () }
        else
            Async.Sleep(100000000)

    let rec producerLoop () = async {
        do! Kafka.Producer.produceBatch producer testTopic [|(jobj [| "x" .= 1|], (string 1))|] |> Async.Ignore
        do! Async.Sleep(1000)
        return! producerLoop()
    }

    // start test producer
    producerLoop()
    |> Async.Start
    
    // start first consumer
    Kafka.Consumer.consume consumerOne laggingHandle 
    |> runWithKafkaMonitor monitorConfig
    |> Async.Start

    do! Async.Sleep(120000)

    // first consumer is only member of group, should have all partitions
    Assert.Equal(4, numPartitions)

    // create second consumer and join group to trigger rebalance
    let consumerTwo = createTestConsumer()
    progressChecked <- false

    do! Async.Sleep(120000)

    // make sure the progress was checked after rebalance
    Assert.True(progressChecked)
    // with second consumer in group, first consumer should have half of the partitions
    Assert.Equal(2, numPartitions)
    consumerOne.ConfluentConsumer.Unsubscribe()
    consumerTwo.ConfluentConsumer.Unsubscribe()
}

[<Fact>]
let ``Monitor should not join consumer group`` () = async {
    let consumer = createTestConsumer()

    let noOpHandler _ = async { () }

    let monitorConfig = createTestMonitorConfig consumer (fun _ _ _ -> ())

    Kafka.Consumer.consume consumer noOpHandler
    |> runWithKafkaMonitor monitorConfig
    |> Async.Start

    do! Async.Sleep(10000)

    // should be one member in group
    Assert.Equal(1, consumer.ConfluentConsumer.ListGroup(testGroup).Members.Count)
    // consumer should have all 4 partitions assigned to it
    Assert.Equal(4, consumer.ConfluentConsumer.Assignment.Count)
    consumer.ConfluentConsumer.Unsubscribe()
}