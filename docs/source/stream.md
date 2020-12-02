# Streaming Feature Sets in Butterfree

## Introduction

Spark enables us to deal with streaming processing in a very powerful way. For an introduction of all Spark's capabilities in streaming you can read more in this [link](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). As the core Spark, Butterfree also let you declare pipelines to deal with streaming data. The best is that the pipeline declaration is almost the same as dealing with batch use-cases, so there isn't too complex to deal with this type of challenge using Butterfree tools.

Streaming feature sets are the ones that have at least one streaming source of data declared in the `Readers` of a `FeatureSetPipeline`. The pipeline is considered a streaming job if it has at least one reader in streaming mode (`stream=True`).

## Readers

Using readers in streaming mode will make use of Spark's `readStream` API instead of the normal `read`. That means it will produce a stream dataframe (`df.isStreaming == True`) instead of a normal Spark's dataframe.

The currently supported readers in stream mode are `FileReader` and `KafkaReader`. For more information about the specifications read their docstrings, [here](https://github.com/quintoandar/butterfree/blob/master/butterfree/extract/readers/file_reader.py#L10) and [here](https://github.com/quintoandar/butterfree/blob/master/butterfree/extract/readers/kafka_reader.py#L12) respectively. 

## Online Feature Store Writer
`OnlineFeatureStoreWriter` is currently the only writer that supports streaming dataframes. It will write, in real time, and upserts to Cassandra. It uses `df.writeStream` and `foreachBatch` Spark functionality to do that. You can read more about `forachBatch` [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch).

![](https://i.imgur.com/KoI1HuC.png)


### Debug Mode
You can use the `OnlineFeatureStoreWriter` in debug mode (`debug_mode=True`) with streaming dataframes. So instead of trying to write to Cassandra, the data will be written to an in-memory table. So you can query this table to show the output as it is being calculated. Normally this functionality is used for the purpose of testing if the defined features have the expected results in real time.

## Pipeline Run
Differently from a batch run, a pipeline running with a streaming dataframe will not "finish to run". The pipeline will continue to get data from the streaming, process the data and save it to the defined sink sources. So when managing a job using this feature, an operation needs to be designed to support a continuously-up streaming job.