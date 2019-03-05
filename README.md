# Jet.ConfluentKafka.FSharp 1.x [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.Jet.ConfluentKafka.FSharp?branchName=v1)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=7?branchName=v1) [![release](https://img.shields.io/github/release/jet/Jet.ConfluentKafka.FSharp.svg)](https://github.com/jet/Jet.ConfluentKafka.FSharp/releases) [![NuGet](https://img.shields.io/nuget/vpre/Jet.ConfluentKafka.FSharp.svg?logo=nuget)](https://www.nuget.org/packages/Jet.ConfluentKafka.FSharp/) [![license](https://img.shields.io/github/license/jet/Jet.ConfluentKafka.FSharp.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/equinox.svg)

F# friendly wrapper for `Confluent.Kafka` versions `>= 1.0.0`, with minimal dependencies or additional abstractions.

See also: [`v0` branch](https://github.com/jet/Jet.ConfluentKafka.FSharp/tree/v0), which targets `Confluent.Kafka` versions `0.9.*` and is designed for compatibility with the [Kafunk](https://github.com/jet/kafunk) API.

## QuickStart

See https://github.com/jet/equinox#quickstart for examples of using this library to Project to Kafka from `Equinox.Cosmos` and/or `Equinox.EventStore`

## Components

The components within this repository are delivered as (presently single) multi-targeted Nuget package targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles

- [![NuGet](https://img.shields.io/nuget/vpre/Jet.ConfluentKafka.FSharp.svg)](https://www.nuget.org/packages/Jet.ConfluentKafka.FSharp/) `Jet.ConfluentKafka.FSharp`: Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations, with basic logging instrumentation. ([depends](https://www.fuget.org/packages/Jet.ConfluentKafka.FSharp) on `Confluent.Kafka >= 1.0.0-beta3`, `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc) and `Newtonsoft.Json` (used internally to parse Statistics for logging purposes)

## CONTRIBUTING

Where it makes sense, raise GitHub issues for any questions so others can benefit from the discussion.

This is an Open Source Project for many reasons, with some central goals:

- quality reference code (the code should be clean and easy to read; where it makes sense, components can be grabbed and cloned locally and used in altered form)
- optimal resilience and performance (getting performance right can add huge value for some systems)
- this code underpins non-trivial production systems (having good tests is not optional for reasons far deeper than having impressive coverage stats)

We'll do our best to be accomodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/Jet.ConfluentKafka.FSharp/issues) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_:

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](quickstart), which walks you through sample code, tuned for approachability, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## BUILDING

Please note the [QuickStart](quickstart) is probably the best way to gain an overview - these instructions are intended to illustrated various facilities of the build script for people making changes. 

NB The tests are reliant on a `TEST_KAFKA_BROKER` environment variable pointing to a test environment that will auto-create ephemeral Kafka Topics as required by the tests (each test run writes to a guid-named topic)

### build, including tests

    dotnet build build.proj

## FAQ

### What is this, why does it exist, where did it come from, is anyone using it ?

This code results from building out an end-to-end batteries-included set of libraries and templates as part of the [Equinox](https://github.com/jet/equinox) project.

Equinox has some key constraints applied to all its components:-

- low dependencies
- no new concepts on top of a standard base library
- thorough test coverage
- batteries-included examples of end-to-end functionality within the Equinox remit _without burdening samples with additional concepts_

You can see [the development process history here](https://github.com/jet/equinox/pull/87). The bulk of the base code is taken from Jet's Warehouse systems (**NB the present version of `Confluent.Kafka` deployed in in production is `0.9.4`**)

### What is Kafunk ?

See https://github.com/jet/kafunk. Kafunk is an F# implementation of a Kafka Client - it's presently unmaintained, and has only trickle of usage within the Jet org, mainly due to the fact that it does not support newer protocol versions.

### What about `jet-confluent-kafka` and `Confluent.Kafka` v `0.9.4` support?

The [`v0` branch](tree/v0) continues to house the original code as previously borne by the `master` of this repo. It's not going anywhere, especially while we have a significant number of `Confluent.Kafka` v `0.9.x` based clients in operation throughout our systems.

- It will continue to address the need to provide an easy migration from the Kafunk API
- There are significant non-trivial changes in lifetime management in the `RdKafka` drivers accompanying `0.9.5` and `0.9.6` (with potential behavioral changes implied too) - while upgrading can likely be achieved without API changes, it does bring into play a series of changes related to how the RdKafka driver closes connections (which can result in long days chasing `AccessViolationException` and friends)
- NB Experience of [the changes necessary to accommodate the sweeping changes that the `Confluent.Kafka` v `1.0.0` API brings when compared to the `0.9.x` codebase](https://github.com/jet/equinox/pull/87) suggests it's likely to be a significant undertaking to wrap `Confluent.Kafka 1.0.0` without significant surface API changes (to go with the behavior changes from the preceding point)

### Whats's involved in migrating from `Jet.ConfluentKafka.fsharp 0.9.x` to `Jet.ConfluentKafka.FSharp 1.0.x` ?

- The producer and consumer API wrappers provide different semantics. It's recommended to validate that they make sense for your use case before considering 'Just Porting'
- Upgrading to a new version of `Confluent.Kafka` always brings with it the knock on effect of taking an increment in the underlying `RdKafka` driver (TODO - explain key behavior and perf changes between what  `1.0.0` implies vs `0.9.4`)
- You need to wire the (`Serilog`-based) logging through to your log sink (it's easy to connect it to an NLog Target etc)
- There's a transitive dependency on `Newtonsoft.Json` v `11.0.2` which should generally not be a problem to accommodate in most codebases