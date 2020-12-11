# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

Please note FsKafka has (generally additive) breaking changes even in Minor and Patch releases as:
- FsKafka binds strongly to a specific version of `Confluent.Kafka` + `librdkafka.redist`,so arbitrary replacements are already frowned on
- a primary goal is to be terse and not vary from the underlying defaults where possible - putting in lots of Obsoleted overloads as one normally should is contrary to this goal

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added
### Changed

- Target [`Confluent.Kafka [1.5.3]`](https://github.com/confluentinc/confluent-kafka-dotnet/blob/v1.5.3/CHANGELOG.md#153), [`librdkafka.redist [1.5.3]`](https://github.com/edenhill/librdkafka/releases/tag/v1.5.3) [#82](https://github.com/jet/FsKafka/pull/82)

### Removed
### Fixed

<a name="1.5.5"></a>
## [1.5.5] - 2020-10-21

### Changed

- Target [`Confluent.Kafka [1.5.2]`](https://github.com/confluentinc/confluent-kafka-dotnet/blob/v1.5.2/CHANGELOG.md#152), [`librdkafka.redist [1.5.2]`](https://github.com/edenhill/librdkafka/releases/tag/v1.5.2) [#80](https://github.com/jet/FsKafka/pull/80)

<a name="1.5.4"></a>
## [1.5.4] - 2020-09-11

### Fixed

- Resolved Memory leak introduced in 1.5.3 [#79](https://github.com/jet/FsKafka/pull/79) :pray: [@wantastic84](https://github.com/wantastic84) 

<a name="1.5.3"></a>
## [1.5.3] - 2020-09-08

### Added

- Logging of consume-loop terminating exceptions caught in the batch processing loop and/or handler [#78](https://github.com/jet/FsKafka/pull/78) :pray: [@wantastic84](https://github.com/wantastic84) 

<a name="1.5.2"></a>
## [1.5.2] - 2020-08-19

### Changed

- BREAKING: Renamed `FsKafka.KafkaMonitor.StartAsChild` to `Start`, changed return type to `IDisposable` [#77](https://github.com/jet/FsKafka/pull/77) :pray: [@wantastic84](https://github.com/wantastic84) 

### Fixed

- Provided mechanism to tear down the monitoring loop when the consumer stops [#77](https://github.com/jet/FsKafka/pull/77) :pray: [@wantastic84](https://github.com/wantastic84) 

<a name="1.5.1"></a>
## [1.5.1] - 2020-08-04

### Added

- BREAKING: `KafkaConsumerConfig.Create`: added `allowAutoCreateTopics` argument to enable control of `allow.auto.create.topics` now that `librdkafka 1.5` changes the default [#71](https://github.com/jet/FsKafka/pull/71)

### Changed

- Changed unit of `maxInFlightBytes` when rendered in logs from GiB to MiB [#72](https://github.com/jet/FsKafka/pull/72)

### Fixed

- Applied `Pause`/`Resume` when consumer quiesces due to `maxInFlightBytes` to avoid `MAXPOLL` condition (not implemented for `FsKafka0`) [#70](https://github.com/jet/FsKafka/pull/70)

<a name="1.5.0"></a>
## [1.5.0] - 2020-07-22

### Changed

- Target [`Confluent.Kafka [1.5.0]`](https://github.com/confluentinc/confluent-kafka-dotnet/releases/tag/v1.5.0), [`librdkafka.redist [1.5.0]`](https://github.com/edenhill/librdkafka/releases/tag/v1.5.0)

<a name="1.4.5"></a>
## [1.4.5] - 2020-07-22

### Changed

- BREAKING: Encapsulated linger/batching semantics in a `Batching` DU passed to `KafkaProducerConfig.Create` (instead of `linger` and `maxInFlight`) in lieu of having `BatchedProducer.CreateWithConfigOverrides` patch the values [#68](https://github.com/jet/FsKafka/pull/68)

### Fixed

- BREAKING: Handle deadlock between `maxInFlightMessageBytes` wait loop and Consumer cancellation [#61](https://github.com/jet/FsKafka/pull/61) :pray: [@bilaldurrani](https://github.com/bilaldurrani)
- `FsKafka0`: Aligned `Thread.Sleep` when over `maxInFlightBytes` threshold with `FsKafka` (reduced from `5` to `1` ms) [#67](https://github.com/jet/FsKafka/pull/67)

<a name="1.4.4"></a>
## [1.4.4] - 2020-06-29

### Added

- Add `.Verbose` log for handler invocations [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@wantastic84](https://github.com/wantastic84) 
- include `partition` property as `LogContext.PushProperty` when dispatching a handler invocation [#60](https://github.com/jet/FsKafka/pull/60)
- `FsKafka0`: Add ConsumerError logging [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@svairagade](https://github.com/svairagade) 

### Changed

- `FsKafka`: Distinguish Fatal Errors from by non-fatal by reducing level to Warning [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@svairagade](https://github.com/svairagade) 
- Target `Confluent.Kafka [1.4.4]`, `librdkafka.redist [1.4.4]`

### Removed

- Remove incorrect Producer logging (it logs before the wait hence generally reports 0), as spotted by [@wantastic84](https://github.com/wantastic84) [#57](https://github.com/jet/FsKafka/pull/57)

### Fixed

- `FsKafka0`: remove `ObjectDisposedException` when doing > 1 of `.Stop` or `.Dispose` on a Consumer [#60](https://github.com/jet/FsKafka/pull/60)
- `FsKafka0`: remove leak due to incorrect tail-recursion (`do!` -> `return!`) (Clone of `FsKafka` [#39](https://github.com/jet/FsKafka/pull/39)) [#59](https://github.com/jet/FsKafka/pull/59)

<a name="1.4.4-rc3"></a>
## [1.4.4-rc3] - 2020-06-25

### Added

- include `partition` property as `LogContext.PushProperty` when dispatching a handler invocation [#60](https://github.com/jet/FsKafka/pull/60)
  
### Changed

- Target `Confluent.Kafka [1.4.4-RC1]`, `librdkafka.redist [1.4.4]`
- Elevated `.Verbose` log for handler invocations from [#57](https://github.com/jet/FsKafka/pull/57) to `.Debug` [#60](https://github.com/jet/FsKafka/pull/60)

### Fixed

- `FsKafka0`: remove `ObjectDisposedException` when doing > 1 of `.Stop` or `.Dispose` on a Consumer [#60](https://github.com/jet/FsKafka/pull/60)

<a name="1.4.4-rc2"></a>
## [1.4.4-rc2] - 2020-06-11

### Fixed

- `FsKafka0`: remove leak due to incorrect tail-recursion (`do!` -> `return!`) (Clone of `FsKafka` [#39](https://github.com/jet/FsKafka/pull/39)) [#59](https://github.com/jet/FsKafka/pull/59)

<a name="1.4.4-rc1"></a>
## [1.4.4-rc1] - 2020-06-10

### Added

- Add `.Verbose` log for handler invocations [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@wantastic84](https://github.com/wantastic84) 
- FsKafka0: Add ConsumerError logging [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@svairagade](https://github.com/svairagade) 

### Changed

- FsKafka: Distinguish Fatal Errors from by non-fatal by reducing level to Warning [#57](https://github.com/jet/FsKafka/pull/57) :pray: [@svairagade](https://github.com/svairagade) 

### Fixed

- Remove incorrect Producer logging (it logs before the wait hence generally reports 0), as spotted by [@wantastic84](https://github.com/wantastic84) [#57](https://github.com/jet/FsKafka/pull/57)

<a name="1.4.3"></a>
## [1.4.3] - 2020-05-20

### Added

- `FsKafka0`: Add `IConsumer`, `ConsumeResult`, `DeliveryReport`, `DeliveryResult` aliases [#55](https://github.com/jet/FsKafka/pull/55)
- Add `Binding.offsetValue` [#56](https://github.com/jet/FsKafka/pull/56)

### Changed

- Target `Confluent.Kafka [1.4.3]`, `librdkafka.redist [1.4.2]`
- `FsKafka0`: Rename `Confluent.Kafka.Config` to `ConfigHelpers` to avoid conflict [#54](https://github.com/jet/FsKafka/pull/54)
- `FsKafka0`: Rename `Confluent.Kafka.Types.*` to `Confluent.Kafka.*` for symmetry [#54](https://github.com/jet/FsKafka/pull/54)

### Fixed

- `FsKafka0`: Replace `AutoOffsetReset.None` with `AutoOffsetReset.Error` [#54](https://github.com/jet/FsKafka/pull/54)

<a name="1.4.2"></a>
## [1.4.2] - 2020-05-13

### Added

- `FsKafka0`: (Moved from [Propulsion.Kafka0](https://github.com/jet/propulsion/tree/ddbcf41072627b26c39ecc915cb747a71ce2a91d/src/Propulsion.Kafka0)) - Implementation of same API as FsKafka based on `Confluent.Kafka` v `0.11.3` [#51](https://github.com/jet/FsKafka/pull/51)
- `KafkaMonitor` based on [Burrow](https://github.com/linkedin/Burrow) (Moved from [Propulsion.Kafka](https://github.com/jet/propulsion/tree/ddbcf41072627b26c39ecc915cb747a71ce2a91d/src/Propulsion.Kafka) [jet/Propulsion #12](https://github.com/jet/propulsion/pull/12) [#51](https://github.com/jet/FsKafka/pull/51) :pray: [@jgardella](https://github.com/jgardella)
- `KafkaProducerConfig.Create`: `requestTimeoutMs` [#52](https://github.com/jet/FsKafka/pull/52)

### Changed

- Target [`Confluent.Kafka`](https://github.com/confluentinc/confluent-kafka-dotnet/releases/tag/v1.4.2) / [`librdkafka.redist`](https://github.com/edenhill/librdkafka/releases/tag/v1.4.2) v `[1.4.2]` [#53](https://github.com/jet/FsKafka/pull/53)
- `KafkaConsumerConfig.Create`: Made `autoOffsetReset` argument mandatory [#52](https://github.com/jet/FsKafka/pull/52)

### Removed

- `Config.validateBrokerUri` [#51](https://github.com/jet/FsKafka/pull/51)

<a name="1.4.1"></a>
## [1.4.1] - 2020-04-18

### Added

- Added optional `headers` param: `KafkaProducer.ProduceAsync(key, value, ?headers)` [#50](https://github.com/jet/FsKafka/pull/50)
- Exposed lower level `KafkaProducer.ProduceAsync(Message<string,string>)` [#50](https://github.com/jet/FsKafka/pull/50)

<a name="1.4.0"></a>
## [1.4.0] - 2020-04-06

### Changed

- *BREAKING*: Replace `broker : Uri` with `bootstrapServers : string` (use `(Config.validateBrokerUri broker)` for backcompat if required) [#49](https://github.com/jet/FsKafka/pull/49)
- Target `Microsoft.NETFramework.ReferenceAssemblies` v `1.0.0`, `Minver` v `2.2.0`
- Updated to `Confluent.Kafka`, `librdkafka.redist` v `1.4.0`
- Updated to SDK `3.1.101`, VM image `macOS-latest`

<a name="1.3.0"></a>
## [1.3.0] - 2019-12-04

### Changed

- Renamed to `FsKafka`
- Updated to `MinVer` v `2.0.0`, `Microsoft.SourceLink.GitHub` v `1.0.0` 
- Updated to `Confluent.Kafka`, `librdkafka.redist` v `1.3.0`

<a name="1.2.0"></a>
## [1.2.0] - 2019-09-21

_NOTE: not interoperable (i.e., via a binding redirect) with CK 1.1 due to a breaking change in the CK 1.2 release, see [#44](https://github.com/jet/FsKafka/pull/44)._

### Added

- `config` argument to `KafaConsumerConfig` and `KafkaProducerConfig` constructors accepting an `IDictionary<string,string>` to match Confluent.Kafka 1.2 ctor overloads [#44](https://github.com/jet/FsKafka/pull/44)

### Changed

- `offsetCommitInterval` renamed to `autoCommitInterval` to match name used in CK >= 1.0 [#45](https://github.com/jet/FsKafka/pull/45)
- Uses `MinVer` v `2.0.0-alpha.2` internally
- Targets [`Confluent.Kafka` v `1.2.0`](https://github.com/confluentinc/confluent-kafka-dotnet/releases/tag/v1.2.0), `librdkafka.redist` v `1.2.0` [#44](https://github.com/jet/FsKafka/pull/44)

<a name="1.1.0"></a>
## [1.1.0] - 2019-06-28

### Changed

- Targets `Confluent.Kafka` v `1.1.0`, `librdkafka.redist` v `1.1.0` [#42](https://github.com/jet/FsKafka/pull/42)
- Added `Producing...` prefix to log messages for consistency
- Made `Consuming...` prefixes for log messages consistent
- Tidied logging of `Unset` values in `Consuming... Committed` messsage
- `ConsumerBuilder.WithLogging` signature change (NB breaking vs `1.0.1`, affects users of [Propulsion libraries](https://github.com/jet/propulsion) only)

### Fixed

- Fixed and clarified defaulting behavior for `fetchMaxBytes`,`maxInFlight`,`partitioner` arguments [#40](https://github.com/jet/FsKafka/pull/40)

<a name="1.0.1"></a>
## [1.0.1] - 2019-06-10

### Removed

- `Propulsion`.* - these have been moved to a dedicated repo:- https://github.com/jet/propulsion

### Fixed

- remove leak due to incorrect tail-recursion (`do!` -> `return!`) [#39](https://github.com/jet/FsKafka/pull/39)

<a name="1.0.1-rc1"></a>

## [1.0.1-rc1] - 2019-06-03

### Added

- `Propulsion.Kafka.Codec.RenderedSpan` (nee `Equinox.Projection.Codec.RenderedSpan`, which is deprecated and is being removed)
- `Propulsion.EventStore`, `Propulsion.Cosmos` (productized from `Equinox.Templates`'s `eqxsync` and `eqxprojector`)

### Changed

- Targets `Confluent.Kafka` v `1.0.1`, `librdkafka.redist` v `1.0.1`

<a name="1.0.0-rc14"></a>
## [1.0.0-rc14] - 2019-06-10

### Fixed

- remove leak due to incorrect tail-recursion (`do!` -> `return!`) (Cherry pick of [#39](https://github.com/jet/FsKafka/pull/39))

<a name="1.0.0-rc13"></a>
## [1.0.0-rc13] - 2019-06-01

### Added

- `StreamsConsumer` and `StreamsProducer` [#35](https://github.com/jet/FsKafka/pull/35)
- `ParallelProducer` [#36](https://github.com/jet/FsKafka/pull/36)

### Changed

- Split reusable components of `ParallelConsumer` out into independent `Propulsion` and `Propulsion.Kafka` libraries [#34](https://github.com/jet/FsKafka/pull/34)

<a name="1.0.0-rc12"></a>
## [1.0.0-rc12] - 2019-05-31

### Added

- Included `totalLag` in Consumer Stats

### Changed

- Default `minInFlightBytes` is now 2/3 of `maxInFlightBytes`
- Reduced `Thread.Sleep` when over `maxInFlightBytes` threshold from `5` to `1` ms 

### Fixed

- Significant tuning / throughput improvements for `ParallelConsumer` 

<a name="1.0.0-rc11"></a>
## [1.0.0-rc11] - 2019-05-27

### Added

- `ParallelConsumer` [#33](https://github.com/jet/FsKafka/pull/33)

### Fixed

- reinstated `AutoOffsetReset` logging in `KafkaConsumerConfig` 

<a name="1.0.0-rc10"></a>
## [1.0.0-rc10] - 2019-05-22

### Added

- mechanism to remove logging regarding polling backoff [#32](https://github.com/jet/FsKafka/pull/32) HT [@szer](https://github.com/Szer) re [#31](https://github.com/jet/FsKafka/issues/31)

### Changed

- split batching behaviors out into `BatchedProducer`/`BatchedConsumer` [#30](https://github.com/jet/FsKafka/pull/30)
- default auto-commit interval dropped from 10s to 5s (which is the `Confluent.Kafka` default) [#30](https://github.com/jet/FsKafka/pull/30)
- removed curried `member` Method arguments in `Start` methods

<a name="1.0.0-rc9"></a>
## [1.0.0-rc9] - 2019-05-22

### Added

- each configuration DSL now has a `customize` function to admit post-processing after defaults and `custom` have taken effect [#29](https://github.com/jet/FsKafka/pull/29)
- Producer/Consumer both have an `Inner` to enable custom logic [#29](https://github.com/jet/FsKafka/pull/29)

### Changed

- default auto-commit interval dropped from 10s to 5s (which is the `Confluent.Kafka` default) [#29](https://github.com/jet/FsKafka/pull/29)
- default `fetchMinBytes` dropped from 10 to 1 (which is the `Confluent.Kafka` default) [#29](https://github.com/jet/FsKafka/pull/29)

<a name="1.0.0-rc8"></a>
## [1.0.0-rc8] - 2019-05-21

### Fixed

- Make custom parameters in consumer config a seq once again [#28](https://github.com/jet/FsKafka/pull/28) [@szer](https://github.com/Szer)

<a name="1.0.0-rc7"></a>
## [1.0.0-rc7] - 2019-05-16

### Added

- Exposed [single-item] `ProduceAsync` in `KafkaProducer`

<a name="1.0.0-rc6"></a>
## [1.0.0-rc6] - 2019-04-24

### Changed

- Updated to target `Confluent.Kafka 1.0.0`

<a name="1.0.0-rc5"></a>
## [1.0.0-rc5] - 2019-04-24

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC7`

<a name="1.0.0-rc4"></a>
## [1.0.0-rc4] - 2019-04-23

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC6`

<a name="1.0.0-rc3"></a>
## [1.0.0-rc3] - 2019-04-12

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC4`

### Fixed

- Cleaned minor logging inconsistency wrt `CompressionType`

<a name="1.0.0-rc2"></a>
## [1.0.0-rc2] - 2019-04-02

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC3` [#24](https://github.com/jet/FsKafka/pull/24)

<a name="1.0.0-rc1"></a>
## [1.0.0-rc1] - 2019-03-27

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC2` (which references `librdkafka.redist 1.0.0`) [#23](https://github.com/jet/FsKafka/pull/23)
- Pins `rdkafka` and `Confluent.Kafka` dependencies to specific known good versions as above [#22](https://github.com/jet/FsKafka/issues/22)

<a name="1.0.0-preview2"></a>
## [1.0.0-preview2] - 2019-03-26

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC1` (triggered relatively minor changes internally due to sane API fixes, does not update to rdkafka 1.0.0, still `1.0.0-RC9`) [#21](https://github.com/jet/FsKafka/pull/21)

<a name="1.0.0-preview1"></a>
## [1.0.0-preview1] - 2019-03-05

(Extracted from [Equinox Changelog](https://github.com/jet/equinox/blob/master/CHANGELOG.md) - this codebase was maintained within that repo originally)

### Added

- `Equinox.Projection.Kafka` consumer metrics emission, see [Equinox #94](https://github.com/jet/equinox/pull/94) @michaelliao5
- Initial release as part of `Equinox.Cosmos` projection facilities, see [Equinox #87](https://github.com/jet/equinox/pull/87) @michaelliao5

<a name="1.0.0-bare"></a>
## [1.0.0-bare]

(Stripped down repo for history purposes, see [`v0` branch](tree/v0) for implementation targeting `Confluent.Kafka` v `0.9.4`)

[Unreleased]: https://github.com/jet/FsKafka/compare/1.5.5...HEAD
[1.5.5]: https://github.com/jet/FsKafka/compare/1.5.4...1.5.5
[1.5.4]: https://github.com/jet/FsKafka/compare/1.5.3...1.5.4
[1.5.3]: https://github.com/jet/FsKafka/compare/1.5.2...1.5.3
[1.5.2]: https://github.com/jet/FsKafka/compare/1.5.1...1.5.2
[1.5.1]: https://github.com/jet/FsKafka/compare/1.5.0...1.5.1
[1.5.0]: https://github.com/jet/FsKafka/compare/1.4.5...1.5.0
[1.4.5]: https://github.com/jet/FsKafka/compare/1.4.4...1.4.5
[1.4.4]: https://github.com/jet/FsKafka/compare/1.4.4-rc3...1.4.4
[1.4.4-rc3]: https://github.com/jet/FsKafka/compare/1.4.4-rc2...1.4.4-rc3
[1.4.4-rc2]: https://github.com/jet/FsKafka/compare/1.4.4-rc1...1.4.4-rc2
[1.4.4-rc1]: https://github.com/jet/FsKafka/compare/1.4.3...1.4.4-rc1
[1.4.3]: https://github.com/jet/FsKafka/compare/1.4.2...1.4.3
[1.4.2]: https://github.com/jet/FsKafka/compare/1.4.1...1.4.2
[1.4.1]: https://github.com/jet/FsKafka/compare/1.4.0...1.4.1
[1.4.0]: https://github.com/jet/FsKafka/compare/1.3.0...1.4.0
[1.3.0]: https://github.com/jet/FsKafka/compare/1.2.0...1.3.0
[1.2.0]: https://github.com/jet/FsKafka/compare/1.1.0...1.2.0
[1.1.0]: https://github.com/jet/FsKafka/compare/1.0.1...1.1.0
[1.0.1]: https://github.com/jet/FsKafka/compare/1.0.1-rc1...1.0.1
[1.0.1-rc1]: https://github.com/jet/FsKafka/compare/1.0.0-rc13...1.0.1-rc1
[1.0.0-rc14]: https://github.com/jet/FsKafka/compare/1.0.0-rc13...1.0.0-rc14
[1.0.0-rc13]: https://github.com/jet/FsKafka/compare/1.0.0-rc12...1.0.0-rc13
[1.0.0-rc12]: https://github.com/jet/FsKafka/compare/1.0.0-rc11...1.0.0-rc12
[1.0.0-rc11]: https://github.com/jet/FsKafka/compare/1.0.0-rc10...1.0.0-rc11
[1.0.0-rc10]: https://github.com/jet/FsKafka/compare/1.0.0-rc9...1.0.0-rc10
[1.0.0-rc9]: https://github.com/jet/FsKafka/compare/1.0.0-rc8...1.0.0-rc9
[1.0.0-rc8]: https://github.com/jet/FsKafka/compare/1.0.0-rc7...1.0.0-rc8
[1.0.0-rc7]: https://github.com/jet/FsKafka/compare/1.0.0-rc6...1.0.0-rc7
[1.0.0-rc6]: https://github.com/jet/FsKafka/compare/1.0.0-rc5...1.0.0-rc6
[1.0.0-rc5]: https://github.com/jet/FsKafka/compare/1.0.0-rc4...1.0.0-rc5
[1.0.0-rc4]: https://github.com/jet/FsKafka/compare/1.0.0-rc3...1.0.0-rc4
[1.0.0-rc3]: https://github.com/jet/FsKafka/compare/1.0.0-rc2...1.0.0-rc3
[1.0.0-rc2]: https://github.com/jet/FsKafka/compare/1.0.0-rc1...1.0.0-rc2
[1.0.0-rc1]: https://github.com/jet/FsKafka/compare/1.0.0-preview2...1.0.0-rc1
[1.0.0-preview2]: https://github.com/jet/FsKafka/compare/1.0.0-preview1...1.0.0-preview2
[1.0.0-preview1]: https://github.com/jet/FsKafka/compare/1.0.0-bare...1.0.0-preview1
[1.0.0-bare]: https://github.com/jet/FsKafka/compare/e4bc8ff53b4f4400308b09c02fe8da6fc7e61d82...1.0.0-bare
