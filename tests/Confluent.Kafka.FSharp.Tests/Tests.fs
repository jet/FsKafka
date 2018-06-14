module ConfluentWrapperTest

open System
open Confluent.Kafka
open NUnit.Framework
open System.Text
open System.Threading
open System.Diagnostics
open FSharp.Control
open System.Collections.Concurrent
open Confluent.Kafka.Config.DebugFlags

/// Produce 10K messages and group by partition
/// Consume messages and make sure that they are consumed in-order and all messages were consumed
[<Test>]
[<Category("Confluent Client")>] 
let ``Message ordering is preserved``() = 

    let host =
        match Environment.GetEnvironmentVariable "CONFLUENT_KAFKA_TEST_BROKER" with
        | x when String.IsNullOrWhiteSpace x -> "localhost"
        | brokers -> brokers
    let topic = "test-topic-partitions"
    let messageCount = 10*1000 
    let reportInterval = TimeSpan.FromSeconds 5.0
    let groupId = "test-group"

    //
    // Producer
    //
    let batchMessagesCount = 2000
    use producer = 
      Config.Producer.safe
      |> Config.bootstrapServers host
      |> Config.clientId "test-client"
      |> Config.Producer.batchNumMessages batchMessagesCount
      |> Config.debug [DebugFlag.Broker]
      |> Producer.create
      |> Producer.onLog (fun logger -> 
        let msg = sprintf "producer: %s" logger.Message
        Debug.WriteLine msg
      )

    let mutable sent = 0
    let producerProcess = async {
      let rnd = new Random()
      use _monitor = 
        let timer = new System.Timers.Timer(reportInterval.TotalMilliseconds)
        do timer.Elapsed |> Observable.subscribe(fun _ -> Debug.WriteLine(">>> {0}", sent)) |> ignore
        timer.Start()
        timer
      let mkMessage (i: int) = 
        let key = Array.zeroCreate 4
        rnd.NextBytes(key)
        let value = BitConverter.GetBytes(i)
        (key, value)

      return! Seq.init messageCount mkMessage
      |> Seq.chunkBySize batchMessagesCount 
      |> AsyncSeq.ofSeq 
      |> AsyncSeq.iterAsync (fun batch -> async {
        let! sentBatch = Producer.produceBatchedBytes producer topic batch
        sent <- sent + sentBatch.Length
        //Debug.WriteLine("Complete batch {0}", sentBatch.Length)
      })
    }

    let producerProcess = async {
      do! producerProcess
      Debug.WriteLine(">>> producerProcess sent: {0}", sent)
    }

    let printCommitted (consumer: Consumer) partitions =
      consumer.Committed(partitions, TimeSpan.FromSeconds(15.0))
      |> Seq.map(fun tpoe -> sprintf "[%d: %d]" tpoe.Partition tpoe.Offset.Value)
      |> String.concat ", "

    //
    // Consumer
    //
    let firstOffsets = new ConcurrentDictionary<int,int64>()
    use consumer = 
      Config.Consumer.safe
      |> Config.bootstrapServers host
      |> Config.Consumer.groupId groupId
      |> Config.clientId "test-client"
      |> Config.Consumer.Topic.autoOffsetReset Config.Consumer.Topic.Beginning
      |> Config.debug [Config.DebugFlags.Consumer; Config.DebugFlags.Cgrp]
      |> Consumer.create
      |> Consumer.onLog(fun logger -> 
        let msg = sprintf "level: %d [%s] [%s]: %s" logger.Level logger.Facility logger.Name logger.Message
        Debug.WriteLine msg
      )

    consumer.OnError
    |> Event.add (fun e -> Debug.WriteLine(e.ToString()))

    consumer.OnConsumeError
    |> Event.add (fun e -> Debug.WriteLine(e.Error.ToString()))

    let mutable assignment = None

    let cancel = new CancellationTokenSource(TimeSpan.FromSeconds(40.0))
    let batchSize = 100
    let mutable count = 0
    // partition -> last seen message value
    let partitions = new ConcurrentDictionary<int, int>()
    let _consumerMonitor = 
      let timer = new System.Timers.Timer(reportInterval.TotalMilliseconds)
      timer.Elapsed |> Observable.subscribe (fun _ -> 
        let committed = printCommitted consumer (consumer.Assignment)

        let positions = 
          consumer.Position(consumer.Assignment)
          |> Seq.map(fun tp -> sprintf "[%d: %d]" tp.Partition tp.Offset.Value)
          |> String.concat ", "

        sprintf "<<< consumer: %d.\n    Committed: %s\n    Positions: %s" count committed positions
        |> Debug.WriteLine
      ) 
      |> ignore

      timer.Start()
    let consumerProcess = 
      Consumer.consume consumer 1000 1000 batchSize (fun batch -> async {
        let firstMsg = batch.messages |> Array.head
        firstOffsets.AddOrUpdate(batch.partition, firstMsg.Offset.Value, (fun _ o -> o)) |> ignore

        batch.messages
        |> Seq.iter(fun msg -> 
          let value = BitConverter.ToInt32(msg.Value, 0)
          partitions.AddOrUpdate(batch.partition, value, 
            fun _p old ->
              Assert.Less(old, value)
              value
          ) |> ignore
          let count' = Interlocked.Increment(&count)
          if count' >= messageCount then
            Debug.WriteLine("Received {0} messages. Cancelling consumer", count')
            cancel.Cancel()
          do ()
        )
      }) 

    let meta = 
      consumer.GetMetadata(true, TimeSpan.FromSeconds(40.0)).Topics
      |> Seq.find(fun t -> t.Topic = topic)
    let offsets =
      meta.Partitions |> Seq.map(fun p -> new TopicPartition(topic, p.PartitionId))
      |> Seq.map(fun tp -> (tp.Partition, consumer.QueryWatermarkOffsets(tp).High.Value))
      |> Map.ofSeq
    Debug.WriteLine("Offsets: {0}", offsets)

    consumer.OnPartitionsAssigned
    |> Event.add (
      fun partitions -> 
        printCommitted consumer partitions
        |> sprintf "OnPartitionsAssigned.Committed: %s"
        |> Debug.WriteLine

        assignment <- Some partitions
        
        consumer.Assign partitions
    )

    consumer.Subscribe topic

    let p1 = 
      [producerProcess; consumerProcess]
      |> Async.Parallel
      |> Async.Ignore

    try
      Async.RunSynchronously( p1, cancellationToken = cancel.Token)
    with 
      | :? OperationCanceledException -> 
        sprintf "First offsets: %s"
        <| String.Join(", ", firstOffsets |> Seq.map(fun i -> sprintf "%d: %d" i.Key i.Value))
        |> Debug.WriteLine

        sprintf "Last offset: %s"
        <| String.Join(", ", partitions |> Seq.map(fun i ->  sprintf "%d: %d" i.Key i.Value))
        |> Debug.WriteLine

        if assignment.IsSome then 
          printCommitted consumer (assignment.Value)
          |> sprintf "Committed after complete: %s"
          |> Debug.WriteLine

        Assert.AreEqual(messageCount, count)

[<Test>]
[<Category("Wrapper")>]
let ``Offsets do not advance until a message is handled`` () =
    let host =
        match Environment.GetEnvironmentVariable "CONFLUENT_KAFKA_TEST_BROKER" with
        | x when String.IsNullOrWhiteSpace x -> "localhost"
        | brokers -> brokers
    let topic = "test-topic-conf-ofsts-dont-advn"
    let groupId = "test-group-conf-ofsts-dont-advn"

    use producer =
      Config.Producer.safe
      |> Config.bootstrapServers host
      |> Config.clientId "test-client"
      |> Producer.create

    let publishOne key'value =
      let producedMsg =
        Confluent.Kafka.Producer.produceString producer topic key'value
        |> Async.RunSynchronously
      Assert.False (producedMsg.Error.HasError, "Failed to publish to topic" + producedMsg.Error.ToString())
      Console.WriteLine ("Published message at partition: {0}, offset: {1}", producedMsg.Partition,  producedMsg.Offset)

    let consumeDuration = TimeSpan.FromSeconds(10.)
    let commitInterval = consumeDuration.TotalMilliseconds / 4.

    use consumer =
      Config.Consumer.safe
      |> Config.bootstrapServers host
      |> Config.Consumer.groupId groupId
      |> Config.clientId "test-client"
      |> Config.Consumer.Topic.autoOffsetReset Config.Consumer.Topic.End
      |> Config.Consumer.commitIntervalMs (int commitInterval)
      |> Config.Consumer.enableAutoCommit true
      |> Consumer.create

    // Create topic if not exists
    do publishOne ("key", "init")

    // Advance offsets to the end of the topic
    let initialHighWatermark =
      let committedOffsets =
        consumer.GetMetadata(true).Topics
        |> Seq.filter (fun m -> m.Topic = topic)
        |> Seq.collect (fun m -> m.Partitions)
        |> Seq.map (fun p ->
          let watermark =
            (topic, p.PartitionId)
            |> TopicPartition
            |> consumer.QueryWatermarkOffsets

          new TopicPartitionOffset(topic, p.PartitionId, watermark.High))
        |> consumer.CommitAsync
        |> Async.AwaitTask
        |> Async.RunSynchronously

      do
        committedOffsets.Offsets
        |> Seq.filter (fun co -> co.Error.HasError)
        |> Seq.map (fun co -> co.Partition)
        |> Assert.IsEmpty

      do Assert.False(committedOffsets.Error.HasError, "COMMIT: " + committedOffsets.Error.ToString())

      committedOffsets.Offsets
      |> Seq.map (fun x -> (x.Topic, x.Partition), x.Offset )
      |> Map.ofSeq

    // Publish one more message which we intend to consume
    do publishOne ("key", "read me")

    // And another that we don't
    do publishOne ("key", "don't read me")

    do consumer.Subscribe topic

    let commitCounter = ref 0
    let offsetDelta = ref 0L

    consumer.OnOffsetsCommitted
    |> Event.add (fun cm ->
      do Interlocked.Increment(commitCounter) |> ignore

      do Assert.False cm.Error.HasError

      cm.Offsets
      |> Seq.iter (fun tpoe ->
        let initOffset = Map.find (tpoe.Topic, tpoe.Partition) initialHighWatermark
        if tpoe.Offset.IsSpecial then
          Console.WriteLine ("Skipping comparison for special offset, partition {0}", tpoe.Partition)
        else
          let newDelta = Interlocked.Add(offsetDelta, (tpoe.Offset.Value - initOffset.Value))
          Assert.AreEqual (1L, newDelta, "Committed offsets should not advance by more than 1!"))
      )

    let cancel = new CancellationTokenSource(consumeDuration)

    let mutable consumeCount = 0
    let consumeProcess = Consumer.consume consumer 100 100 1 (fun _ -> async {
      if consumeCount = 0 then
        consumeCount <- consumeCount + 1
      else
        // Don't actually process the message, but sleep for twice the consume duration
        let sleepDuration = consumeDuration.Add(consumeDuration)
        do! Async.Sleep (int sleepDuration.TotalMilliseconds)
        failwith "Did not sleep long enough"
      return () })

    try
      do Async.RunSynchronously (consumeProcess, cancellationToken = cancel.Token)
    with
      | :? OperationCanceledException -> Assert.Greater (!commitCounter, 0, "No periodic commits happened")
