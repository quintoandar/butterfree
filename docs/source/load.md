# Sink

The Load step is the `Sink` method, where we define the destinations for the feature set pipeline, that is, it is the process of recording the transformed data after the transformation step.

Declaring the sink:
```python
sink = Sink(
           writers = [HistoricalFeatureStoreWriter(), OnlineFeatureStoreWriter()]
       ),
```

Currently, you can write your data into two types of `writers`:

* `HistoricalFeatureStoreWriter`: The Historical Feature Store will write the data to an AWS S3 bucket.

* `OnlineFeatureStoreWriter`: The Online Feature Store will write the data to a Cassandra database.

If you declare your writers without a database configuration, they will use their default settings. But we can also define this configuration, such as:

* `HistoricalFeatureStoreWriter`: 
```python
config = S3Config(bucket="my_bucket", mode="append", format_="parquet")
writers = [HistoricalFeatureStoreWriter(db_config=config)]
```

* `OnlineFeatureStoreWriter`:
```python
config = CassandraConfig(
                      mode="overwrite", 
                      format_="org.apache.spark.sql.cassandra", 
                      keyspace="keyspace_name"
         )
writers = [OnlineFeatureStoreWriter(db_config=config)]
`````

You can see the writers [here](https://github.com/quintoandar/butterfree/tree/staging/butterfree/core/load/writers) and database configuration [here](https://github.com/quintoandar/butterfree/tree/staging/butterfree/core/configs/db).

It's also important to highlight that our writers support a ```debug_mode``` option:
```python
writers = [HistoricalFeatureStoreWriter(debug_mode=True), OnlineFeatureStoreWriter(debug_mode=True)]
sink = Sink(writers=writers)
```
When ```debug_mode``` assumes ```True```, then a temporary view will be created, therefore no data will be actually saved to both historical and online feature store. Feel free to check our [examples section](https://github.com/quintoandar/butterfree/tree/staging/examples), in order to learn more about how to use this mode.