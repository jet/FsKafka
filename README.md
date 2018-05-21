# Confluent.Kafka.FSharp

An thin, F# friendly wrapper for Confluent.Kafka, designed for compatibility with the Kafunk API. This is a work in progress.

## Using the wrapper

To incorporate the wrapper in your project place the following line in your paket.dependencies file:
```
github jet/confluent-kafka-fsharp:<commit hash> src/Confluent.Kafka.FSharp/ConfluentKafka.fs
```
and in paket.references:
```
File: ConfluentKafka.fs
```
Your project would need to additionally reference the following nuget dependencies:
```
Confluent.Kafka
System.Reactive
NLog
```

## Running Tests

Make sure you set the `CONFLUENT_KAFKA_TEST_BROKER` environment variable to an appropriate kafka broker before running the tests.
