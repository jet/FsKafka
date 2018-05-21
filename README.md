# Confluent.Kafka.FSharp

An thin, F# friendly wrapper for Confluent.Kafka, designed for compatibility with the Kafunk API. This is a work in progress.

## Running Tests

Make sure you set the `CONFLUENT_KAFKA_TEST_BROKER` environment variable to an appropriate kafka broker before running the tests.

## Desired features from C# driver
* Producer support of asynchronous buffer overflow (pushback)
* Consumer: support of asynchronous message handler