# FsKafka [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.FsKafka)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=7) [![release](https://img.shields.io/github/release/jet/FsKafka.svg)](https://github.com/jet/FsKafka/releases) [![NuGet](https://img.shields.io/nuget/v/FsKafka.svg?logo=nuget)](https://www.nuget.org/packages/FsKafka/) [![license](https://img.shields.io/github/license/jet/FsKafka.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/FsKafka.svg)

F# friendly wrapper for `Confluent.Kafka`, with minimal dependencies or additional abstractions (but see [related repos](#related-repos)). 

`FsKafka` wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations with basic logging instrumentation. [Depends](https://www.fuget.org/packages/FsKafka) on `Confluent.Kafka [1.8.1]`, `librdkafka [1.8.1]` (pinned to ensure we use a tested pairing), `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc) and `Newtonsoft.Json` (used internally to parse Broker-provided Statistics for logging purposes).

## Usage

FsKafka is delivered as a [Nuget package](https://www.nuget.org/packages/FsKafka/) targeting `netstandard2.0` and F# >= 4.5.

```powershell
Install-Package FsKafka
```

or for `paket`, use:

```bash
paket add FsKafka
```

## Related repos

- See [the Propulsion repo](https://github.com/jet/propulsion) for extended Producers and Consumers.
- See [the Jet `dotnet new` templates repo](https://github.com/jet/dotnet-templates)'s `proProjector` template (in `-k` mode) for example producer logic using the `BatchedProducer` and the `proConsumer` template for examples of using the `BatchedConsumer` from `FsKafka`, alongside the extended modes in `Propulsion`.
- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

## CONTRIBUTING

Contributions of all sizes are warmly welcomed. See [our contribution guide](CONTRIBUTING.md)

## TEMPLATES

The best place to start, sample-wise is from the `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

The [templates](#templates) are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes.

NB The tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run writes to a guid-named topic)

### build, including tests on netcoreapp3.1

```powershell
export TEST_KAFKA_BROKER="<server>:9092"
dotnet build build.proj -v n
```

## FAQ

### How do I get rid of all the `breaking off polling` ... `resuming polling` spam?

- The `BatchedConsumer` implementation tries to give clear feedback as to when reading is not keeping up, for diagnostic purposes. As of [#32](https://github.com/jet/FsKafka/pull/32), such messages are tagged with the type `FsKafka.Core.InFlightMessageCounter`, and as such can be silenced by including the following in one's `LoggerConfiguration()`: 

    `.MinimumLevel.Override(FsKafka.Core.Constants.messageCounterSourceContext, Serilog.Events.LogEventLevel.Warning)`

### What is this, why does it exist, where did it come from, is anyone using it ?

This code results from building out an end-to-end batteries-included set of libraries and templates as part of the [Equinox](https://github.com/jet/equinox) project.

Equinox places some key constraints on all components and dependencies:-

- batteries-included examples of end-to-end functionality within the Equinox remit; _samples should have clean consistent wiring_
- pick a well-established base library, try not to add new concepts
- low dependencies, so it can work in lots of contexts without egregiously forcing you to upgrade things
- aim to add any resilience features as patches to upstream repos
- thorough test coverage; integration coverage for core wrapped functionality, unit tests for any non-trivial logic in the wrapper library 

## Minimal producer example

```fsharp
#r "nuget:FsKafka"
open Confluent.Kafka
open FsKafka

let log = Serilog.LoggerConfiguration().CreateLogger()

let batching = Batching.Linger (System.TimeSpan.FromMilliseconds 10.)
let producerConfig = KafkaProducerConfig.Create("MyClientId", "kafka:9092", Acks.All, batching)
let producer = KafkaProducer.Create(log, producerConfig, "MyTopic")
   
let key = Guid.NewGuid().ToString()
let deliveryResult = producer.ProduceAsync(key, "Hello World!") |> Async.RunSynchronously
```

## Minimal batched consumer example

```fsharp
#r "nuget:FsKafka"
open Confluent.Kafka
open FsKafka

let log = Serilog.LoggerConfiguration().CreateLogger()

let handler (messages : ConsumeResult<string,string> []) = async {
    for m in messages do
        printfn "Received: %s" m.Message.Value
} 

let cfg = KafkaConsumerConfig.Create("MyClientId", "kafka:9092", ["MyTopic"], "MyGroupId", AutoOffsetReset.Earliest)

async {
    use consumer = BatchedConsumer.Start(log, cfg, handler)
    return! consumer.AwaitShutdown()
} |> Async.RunSynchronously
```

## Minimal batched consumer example with monitor

```fsharp
#r "nuget:FsKafka"
open Confluent.Kafka
open FsKafka

let log = Serilog.LoggerConfiguration().CreateLogger()

let handler (messages : ConsumeResult<string,string> []) = async {
    for m in messages do
        printfn "Received: %s" m.Message.Value
} 

let cfg = KafkaConsumerConfig.Create("MyClientId", "kafka:9092", ["MyTopic"], "MyGroupId", AutoOffsetReset.Earliest)

async {
    use consumer = BatchedConsumer.Start(log, cfg, handler)
    use _ = KafkaMonitor(log).Start(consumer.Inner, cfg.Inner.GroupId)
    return! consumer.AwaitShutdown()
} |> Async.RunSynchronously
```

## Running (and awaiting) a pair of consumers until either throws

```fsharp
#r "nuget:FsKafka"
open Confluent.Kafka
open FsKafka

let log = Serilog.LoggerConfiguration().CreateLogger()

let handler (messages : ConsumeResult<string,string> []) = async {
    for m in messages do
        printfn "Received: %s" m.Message.Value
} 

let config topic = KafkaConsumerConfig.Create("MyClientId", "kafka:9092", [topic], "MyGroupId", AutoOffsetReset.Earliest)

let cfg1, cfg2 = config "MyTopicA", config "MyTopicB"

async {
    use consumer1 = BatchedConsumer.Start(log, cfg1, handler)
    use consumer2 = BatchedConsumer.Start(log, cfg2, handler)
    use _ = KafkaMonitor(log).Start(consumer1.Inner, cfg1.Inner.GroupId)
    use _ = KafkaMonitor(log).Start(consumer2.Inner, cfg2.Inner.GroupId)
    return! Async.Parallel [consumer1.AwaitWithStopOnCancellation(); consumer2.AwaitWithStopOnCancellation()]
} |> Async.RunSynchronously
```
