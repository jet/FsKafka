# FsKafka [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.FsKafka)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=7) [![release](https://img.shields.io/github/release/jet/FsKafka.svg)](https://github.com/jet/FsKafka/releases) [![NuGet](https://img.shields.io/nuget/v/FsKafka.svg?logo=nuget)](https://www.nuget.org/packages/FsKafka/) [![license](https://img.shields.io/github/license/jet/FsKafka.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/FsKafka.svg)

F# friendly wrapper for `Confluent.Kafka` versions `>= 1.3.0`, with minimal dependencies or additional abstractions.

## Components

The components within this repository are delivered as a multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- [![NuGet](https://img.shields.io/nuget/v/FsKafka.svg)](https://www.nuget.org/packages/FsKafka/) `FsKafka`: Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations with basic logging instrumentation. [Depends](https://www.fuget.org/packages/FsKafka) on `Confluent.Kafka [1.2.0]`, `librdkafka [1.2.0]` (pinned to ensure we use a tested pairing enterprise wide), `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc) and `Newtonsoft.Json` (used internally to parse Broker-provided Statistics for logging purposes).

## Related repos

- See [the Propulsion repo](https://github.com/jet/propulsion) for extended Producers and Consumers.
- See [the Jet `dotnet new` templates repo](https://github.com/jet/dotnet-templates)'s `proProjector` template (in `-k` mode) for example producer logic using the `BatchedProducer` and the `proConsumer` template for examples of using the `BatchedConsumer` from `FsKafka`, alongside the extended modes in `Propulsion`.
- See [the Equinox QuickStart](https://github.com/jet/equinox#quickstart) for examples of using this library to project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`.

## CONTRIBUTING

Please raise GitHub issues for any questions so others can benefit from the discussion.

This is an Open Source project for many reasons, with some central goals:

- quality reference code (the code should be clean and easy to read; where it makes sense, it should remain possible to clone it locally and use it in tweaked form)
- optimal resilience and performance (getting performance right can add huge value for some systems, i.e., making it prettier but disrupting the performance would be bad)
- this code underpins non-trivial production systems (having good tests is not optional for reasons far deeper than having impressive coverage stats)

We'll do our best to be accomodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/FsKafka/issues) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](#quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

Please note the [QuickStart](#quickstart) is probably the best way to gain an overview, and the templates are the best way to see how to consume it; these instructions are intended mainly for people looking to make changes. 

NB The tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a Broker that has been configured to auto-create ephemeral Kafka Topics as required by the tests (each test run writes to a guid-named topic)

### build, including tests on net461 and netcoreapp2.1

```powershell
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

### What is Kafunk ?

[Kafunk](https://github.com/jet/kafunk) is an F# implementation of a Kafka Client - it's presently unmaintained (and has only trickle of usage within the Jet org, chiefly due to the fact that it does not support newer protocol versions necessitated by our broker configurations).

### What about `jet-confluent-kafka` and `Confluent.Kafka` v `0.11.4` support?

The [`v0` branch](tree/v0) continues to house the original code as previously borne by the `master` of this repo

- It will continue to address the need to provide an easy migration from the Kafunk API
- There are significant non-trivial changes in lifetime management in the `librdafka` drivers accompanying `0.11.5` and `0.11.6` (with potential behavioral changes implied too). While upgrading may be achievable without API changes, it does bring into play a series of changes related to how the `rdkafka` driver closes connections (which can result in long days chasing `AccessViolationException` and friends)
- **NB Experience of [the changes necessary to accommodate the sweeping changes that the `Confluent.Kafka` v `1.0.0` API brings when compared to the `0.11.x` codebase](https://github.com/jet/equinox/pull/87) suggests it's likely to be a significant undertaking to adjust the `v0` branch to target `Confluent.Kafka` v `>= 1.0.0` without significant surface API changes (TL;DR [there are no more events, you need to wire everything up in the Builder](https://github.com/confluentinc/confluent-kafka-dotnet/issues/994)); there is absolutely no plan or resourcing to introduce such changes on the `v0` branch; the suggested upgrade path is using the shims in `Propulsion.Kafka0` in order to do an incremental switch-over to v1**

### Whats's involved in migrating from `Jet.ConfluentKafka.fsharp 0.9.x` to `FsKafka 1.0.x` ?

- the producer and consumer API wrappers provide different semantics to the `v0` branch. It's recommended to validate that they make sense for your use case
- upgrading to a new version of `Confluent.Kafka` typically implies a knock on effect from an associated increment in the underlying `rdkafka` driver version (TODO - explain key behavior and perf changes between what  `1.0.0` implies vs `0.11.4`)
- you'll need to wire the (`Serilog`-based) logging through to your log sink (it's easy to connect it to an NLog Target etc). (The `v0` branch exposes a logging scheme which requires more direct integration)
- there's a transitive dependency on `Newtonsoft.Json` v `11.0.2` (which should generally not be a problem to accommodate in most codebases)

## Minimal example

```fsharp
#r "nuget:FsKafka"
open FsKafka
open Confluent.Kafka
    
let producerConfig = KafkaProducerConfig.Create("MyClientId", Uri("kafka:9092"), Acks.All)
let producer = KafkaProducer.Create(Serilog.LoggerConfiguration().CreateLogger(), producerConfig, "MyTopic")   
let key = Guid.NewGuid().ToString()
let produced = producer.ProduceAsync(key, "Hello World!) |> Async.RunSynchronously
```
