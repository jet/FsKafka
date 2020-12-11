# FsKafka [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.FsKafka)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=7) [![release](https://img.shields.io/github/release/jet/FsKafka.svg)](https://github.com/jet/FsKafka/releases) [![NuGet](https://img.shields.io/nuget/v/FsKafka.svg?logo=nuget)](https://www.nuget.org/packages/FsKafka/) [![license](https://img.shields.io/github/license/jet/FsKafka.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/FsKafka.svg)

F# friendly wrapper for `Confluent.Kafka`, with minimal dependencies or additional abstractions (but see [related repos](#related-repos)). Includes variant based on `Confluent.Kafka` v `0.11.3` as a way to manage migration from `0.x` to `1.x`.

## Components

The components within this repository are delivered as a multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- [![NuGet](https://img.shields.io/nuget/v/FsKafka.svg)](https://www.nuget.org/packages/FsKafka/) `FsKafka`: Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations with basic logging instrumentation. [Depends](https://www.fuget.org/packages/FsKafka) on `Confluent.Kafka [1.5.3]`, `librdkafka [1.5.3]` (pinned to ensure we use a tested pairing), `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc) and `Newtonsoft.Json` (used internally to parse Broker-provided Statistics for logging purposes).
- [![NuGet](https://img.shields.io/nuget/v/FsKafka0.svg)](https://www.nuget.org/packages/FsKafka0/) `FsKafka0`: As per `FsKafka`; [Depends](https://www.fuget.org/packages/FsKafka0) on `Confluent.Kafka [0.11.3]`, `librdkafka [0.11.4]`, `Serilog` and `Newtonsoft.Json`.

## Related repos

- See [the Propulsion repo](https://github.com/jet/propulsion) for extended Producers and Consumers.
- See [the Jet `dotnet new` templates repo](https://github.com/jet/dotnet-templates)'s `proProjector` template (in `-k` mode) for example producer logic using the `BatchedProducer` and the `proConsumer` template for examples of using the `BatchedConsumer` from `FsKafka`, alongside the extended modes in `Propulsion`.
- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

## CONTRIBUTING

See [CONTRIBUTING.md](CONTRIBUTING.md)

## TEMPLATES

The best place to start, sample-wise is from the `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

The [templates](#templates) are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes.

NB The tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run writes to a guid-named topic)

### build, including tests on net461 and netcoreapp2.1

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
- low dependencies, so it can work in lots of contexts without eggregiously forcing you to upgrade things
- aim to add any resilience features as patches to upstream repos
- thorough test coverage; integration coverage for core wrapped functionality, unit tests for any non-trivial logic in the wrapper library 

You can see [the development process history here](https://github.com/jet/equinox/pull/87). The base code originates from Jet's Inventory Management System (at the point it was taken, it worked against `Confluent.Kafka` v `0.11.4`)

### What about `jet-confluent-kafka` and `Confluent.Kafka` v `0.11.4` support?

The [`v0` branch](tree/v0) continues to house the original code as previously borne by the `master` of this repo

- It will continue to address the need to provide an easy migration from the Kafunk API
- There are significant non-trivial changes in lifetime management in the `librdafka` drivers accompanying `0.11.5` and `0.11.6` (with potential behavioral changes implied too). While upgrading may be achievable without API changes, it does bring into play a series of changes related to how the `rdkafka` driver closes connections (which can result in long days chasing `AccessViolationException` and friends)
- **NB Experience of [the changes necessary to accommodate the sweeping changes that the `Confluent.Kafka` v `1.0.0` API brings when compared to the `0.11.x` codebase](https://github.com/jet/equinox/pull/87) suggests it's likely to be a significant undertaking to adjust the `v0` branch to target `Confluent.Kafka` v `>= 1.0.0` without significant surface API changes (TL;DR [there are no more events, you need to wire everything up in the Builder](https://github.com/confluentinc/confluent-kafka-dotnet/issues/994)); there is absolutely no plan or resourcing to introduce such changes on the `v0` branch; the suggested upgrade path is using the shims in `Propulsion.Kafka0` in order to do an incremental switch-over to v1**

### What is/was Kafunk ?

[Kafunk](https://github.com/jet/kafunk) was an end-to-end F# implementation of a Kafka Client; it's no longer in active use in Jet.com

### Whats's involved in migrating from `Jet.ConfluentKafka.fsharp 0.9.x` to `FsKafka 1.0.x` ?

- the producer and consumer API wrappers provide different semantics to the `v0` branch. It's recommended to validate that they make sense for your use case
- upgrading to a new version of `Confluent.Kafka` typically implies a knock on effect from an associated increment in the underlying `rdkafka` driver version (TODO - explain key behavior and perf changes between what  `1.0.0` implies vs `0.11.4`)
- you'll need to wire the (`Serilog`-based) logging through to your log sink (it's easy to connect it to an NLog Target etc). (The `v0` branch exposes a logging scheme which requires more direct integration)
- there's an (internal) transitive dependency on `Newtonsoft.Json` v `11.0.2` (which should generally not be a problem to accommodate in most codebases)

### Sounds like a lot of work; surely there's a better way?

The single recommended way to move off a 0.x dependency that uses the older style API is to:
1. retarget your code to use `FsKafka0`
2. release, test, validate
3. retarget your code to use `FsKafka`

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

## Running (and awaiting) a pair of consumers until either abends

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
