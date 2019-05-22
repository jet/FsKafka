# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added

- split batching behaviors out into `BatchedProducer`/`BatchedConsumer` [#30](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/30)

### Changed

- default auto-commit interval dropped from 10s to 5s (which is the `Confluent.Kafka` default) [#30](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/30)
- removed curried `member` Method arguments in `Start` methods

### Removed
### Fixed

<a name="1.0.0-rc9"></a>
## [1.0.0-rc9] - 2019-05-22

### Added

- each configuration DSL now has a `customize` function to admit post-processing after defaults and `custom` have taken effect [#29](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/29)
- Producer/Consumer both have an `Inner` to enable custom logic [#29](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/29)

### Changed

- default auto-commit interval dropped from 10s to 5s (which is the `Confluent.Kafka` default) [#29](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/29)
- default `fetchMinBytes` dropped from 10 to 1 (which is the `Confluent.Kafka` default) [#29](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/29)

<a name="1.0.0-rc8"></a>
## [1.0.0-rc8] - 2019-05-21

### Fixed

- Make custom parameters in consumer config a seq once again [#28](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/28) [@szer](https://github.com/Szer)

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

- Updated to target `Confluent.Kafka 1.0.0-RC3` [#24](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/24)

<a name="1.0.0-rc1"></a>
## [1.0.0-rc1] - 2019-03-27

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC2` (which references `librdkafka.redist 1.0.0`) [#23](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/23)
- Pins `rdkafka` and `Confluent.Kafka` dependencies to specific known good versions as above [#22](https://github.com/jet/Jet.ConfluentKafka.FSharp/issues/22)

<a name="1.0.0-preview2"></a>
## [1.0.0-preview2] - 2019-03-26

### Changed

- Updated to target `Confluent.Kafka 1.0.0-RC1` (triggered relatively minor changes internally due to sane API fixes, does not update to rdkafka 1.0.0, still `1.0.0-RC9`) [#21](https://github.com/jet/Jet.ConfluentKafka.FSharp/pull/21)

<a name="1.0.0-preview1"></a>
## [1.0.0-preview1] - 2019-03-05

(Extracted from [Equinox Changelog](https://github.com/jet/equinox/blob/master/CHANGELOG.md) - this codebase was maintained within that repo originally)

### Added

- `Equinox.Projection.Kafka` consumer metrics emission, see [Equinox #94](https://github.com/jet/equinox/pull/94) @michaelliao5
- Initial release as part of `Equinox.Cosmos` projection facilities, see [Equinox #87](https://github.com/jet/equinox/pull/87) @michaelliao5

<a name="1.0.0-bare"></a>
## [1.0.0-bare]

(Stripped down repo for history purposes, see [`v0` branch](tree/v0) for implementation targeting `Confluent.Kafka` v `0.9.4`)

[Unreleased]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc9...HEAD
[1.0.0-rc9]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc8...1.0.0-rc9
[1.0.0-rc8]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc7...1.0.0-rc8
[1.0.0-rc7]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc6...1.0.0-rc7
[1.0.0-rc6]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc5...1.0.0-rc6
[1.0.0-rc5]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc4...1.0.0-rc5
[1.0.0-rc4]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc3...1.0.0-rc4
[1.0.0-rc3]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc2...1.0.0-rc3
[1.0.0-rc2]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-rc1...1.0.0-rc2
[1.0.0-rc1]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-preview2...1.0.0-rc1
[1.0.0-preview2]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-preview1...1.0.0-preview2
[1.0.0-preview1]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/1.0.0-bare...1.0.0-preview1
[1.0.0-bare]: https://github.com/jet/Jet.ConfluentKafka.FSharp/compare/e4bc8ff53b4f4400308b09c02fe8da6fc7e61d82...1.0.0-bare