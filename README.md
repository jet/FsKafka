# Confluent.Kafka.FSharp

F# friendly wrapper for Confluent.Kafka, designed for compatibility with the Kafunk API.

## Using the wrapper

To incorporate the wrapper in your project place the following line in your paket.dependencies file:
```
github jet/confluent-kafka-fsharp:<commit hash> src/Confluent.Kafka.FSharp/ConfluentKafka.fs
nuget Confluent.Kafka

```
and in paket.references:
```
File: ConfluentKafka.fs
Confluent.Kafka
```

## Running Tests

Make sure you set the `CONFLUENT_KAFKA_TEST_BROKER` environment variable to an appropriate kafka broker before running the tests.

## Differences with kafunk
### Conceptual changes
Because differences between kafunk and confluent can not be masked reasonably cheap, there are some differences in behaviour. Creating a client is now synchronous process and actual connect to brokers will happen upon first request whereas in kafunk it was during create call.

Configuration changes require the most rework beacuse it is reflecting confluent/java API and does not have separate channel and kafka configuration anymore.
Kafka connection object is not exposed by confluent as a standalone object.
Low-level API such as Fetch Request or protocol-level Kafka message size is not exposed by Confluent too.

Confluent's driver has some settings by default, which does not guarantee "at least once" or "in-order" delivery. See Configuration below to start with safe settings.

### Configuration
Configuration is simplified. No more separate channel configuration. Most used configs are made strong typed and rest can be used as string tuple. For full list of configuration options, see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

Wrapper's configuration is set by default to set "at least once" and 'in order" delivery. Also, Confluent consumer might skip messages when configured by default: https://github.com/confluentinc/confluent-kafka-dotnet/issues/527

Once configured, you can call Consumer|Producer.create and will be given Confluent's client instance.

Producer:
https://github.com/jet/confluent-kafka-fsharp/blob/911e4775211044a43c7df3122a7fca1b536313e9/tests/Confluent.Kafka.FSharp.Tests/Tests.fs#L32

Consumer:
https://github.com/jet/confluent-kafka-fsharp/blob/911e4775211044a43c7df3122a7fca1b536313e9/tests/Confluent.Kafka.FSharp.Tests/Tests.fs#L82

### Metadata
Use `GetMetadata` call: https://github.com/confluentinc/confluent-kafka-dotnet/blob/57192f3122569de257841b4057ea79ad4107ce10/src/Confluent.Kafka/Consumer.cs#L688
Both, Consumer and Producer have this call implemented.

Note: `GetMetadata` API is confusing. If call `GetMetadata(allTopics: false)`, you might expect to get metadata for topic, which client was configured for. Instead you will get something called "locally known topics" metadata. Those are no topics at all, if client have not connected to broker yet or topics which include kafka system topics (offsets persistence topics), which will wreck havoc in your metadata processing logic.
Instead call `GetMetadata(allTopics: true)` and filter out topic of interest. 

### Fetching high watermark (last offset)
You can either get offsets which driver is currently aware of `GetWatermarkOffsets` (is being updated with every received message) or issue a query to broker to get high/low offsets `QueryWatermarkOffsets`.
https://github.com/confluentinc/confluent-kafka-dotnet/blob/57192f3122569de257841b4057ea79ad4107ce10/src/Confluent.Kafka/Consumer.cs#L653

### Consumer group info
Use `ListGroup` call:
https://github.com/confluentinc/confluent-kafka-dotnet/blob/57192f3122569de257841b4057ea79ad4107ce10/src/Confluent.Kafka/Consumer.cs#L594

### Consuming at explicit offsets and (re)setting offsets
Confluent driver has Consumer.OnPartitionsAssigned event. In this event you are provided with `List<TopicPartition>` which your consumer is assigned. Note, there are no offsets. The idea is that by default, in this handler you would call `Consumer.Assign` with provided list and driver will figure out last committed offsets for you:
https://github.com/confluentinc/confluent-kafka-dotnet/blob/57192f3122569de257841b4057ea79ad4107ce10/examples/AdvancedConsumer/Program.cs#L88

But if you write a tool and need to set all partitions to certain offset, there is another override to `Consumer.Assign`, which takes `List<TopicPartitionOffset>`. Or, if you want to set offsets but are not intendent to consume, call `Consumer.CommitAsync` For example, [see this script](./blob/master/src/Confluent.Kafka.FSharp/Script.fsx):

## Wrapper vs using Confluent kafka directly
Wrapper address the following concerns:
* Provide Confluent client configured with safe defaults ("at least once" and in-order delivery)
* "Legacy" module which mimic kafunk API as much as possible

If you know what you are doing, you are free to use Confluent Kafka directly, just be ready to spend some time on learning its safe configurations (there is lack of documentation, most info is in github issues).