# Setup Configuration

To perform a ETL pipeline, some configurations are needed. For example: credentials to connect to data sources and default paths. This chapter will cover the main setup configurations on Butterfree.

Some of the configurations are possible to set trough environment variables, the API or both. The priority is first trying to use the parameters used in the instantiation, with the environment variables as fallback. Check the following examples:

### Kafka Reader
Configurations:
 - Kafka Consumer Connection String: connection string to use to connect to the Kafka.

#### Setup by instantiation:
```python
from butterfree.core.extract.readers import KafkaReader

kafka_reader = KafkaReader(
    id="reader_id",
    topic="topic_name",
    value_schema=schema,
    connection_string="host1:port,host2:port"
)
```

#### Setup by environment variables:
Setup the following environment variable in the Spark Cluster/Locally:
`KAFKA_CONSUMER_CONNECTION_STRING`

After setting this variables you can instantiate `KafkaReader` easier:
```python
from butterfree.core.extract.readers import KafkaReader

kafka_reader = KafkaReader(
    id="reader_id",
    topic="topic_name",
    value_schema=schema
)

```

### Online Feature Store (Cassandra Configuration)

Configurations:
 - Cassandra Username: username to connect to CassandraDB.

 - Cassandra Password: password to connect to CassandraDB.

 - Cassandra Host: host to connect to CassandraDB.

 - Cassandra Keyspace: keyspace to connect to CassandraDB.

 - Stream Checkpoint Path: path on S3 to save the checkpoint for the streaming query. Only need when performing streaming writes.

#### Setup by instantiation:
```python
from butterfree.core.configs.db import CassandraConfig
from butterfree.core.load.writers import OnlineFeatureStoreWriter

db_config  = CassandraConfig(
    username="username", 
    password="password",
    host="host",
    keyspace="keyspace",
    stream_checkpoint_path="path"
)
writer = OnlineFeatureStoreWriter(db_config=db_config)
```

#### Setup by environment variables:
Setup the following environment variables in the Spark Cluster/Locally:
`CASSANDRA_USERNAME`, `CASSANDRA_PASSWORD`, `CASSANDRA_HOST`, `CASSANDRA_KEYSPACE`, `STREAM_CHECKPOINT_PATH`

After setting this variables you can instantiate `CassandraConfig` and `OnlineFeatureStoreWriter` easier:
```Python
from butterfree.core.configs.db import CassandraConfig
from butterfree.core.load.writers import OnlineFeatureStoreWriter

db_config  = CassandraConfig()
writer = OnlineFeatureStoreWriter(db_config=db_config)

# or just
writer = OnlineFeatureStoreWriter()
```

### Historical Feature Store (Spark Metastore and S3)
Configurations:
 - Feature Store S3 Bucket: Bucket on S3 to use for the Historical Feature Store.

 - Feature Store Historical Database: Database on Spark metastore to use to write the tables from Historical Feature Store.

#### Setup by instantiation:
```python
from butterfree.core.configs.db import S3Config
from butterfree.core.load.writers import HistoricalFeatureStoreWriter

db_config  = S3Config(bucket="bucket")
writer = HistoricalFeatureStoreWriter(
    db_config=db_config,
    database="database"
)
```

#### Setup by environment variables:
Setup the following environment variables in the Spark Cluster/Locally:
`FEATURE_STORE_S3_BUCKET`, `FEATURE_STORE_HISTORICAL_DATABASE`

After setting this variables you can instantiate `S3Config` and `HistoricalFeatureStoreWriter` easier:
```Python
from butterfree.core.configs.db import S3Config
from butterfree.core.load.writers import HistoricalFeatureStoreWriter

db_config  = S3Config()
writer = HistoricalFeatureStoreWriter(db_config=db_config)

# or just
writer = HistoricalFeatureStoreWriter()
```

### Important Considerations
#### API information
For more detailed information on the API arguments is better to check their own docstrings.

#### Troubleshoot checks
- Check if Spark cluster machines have all the permissions to read/write/modify on the Historical FeatureStore Bucket
- Check if Spark cluster machines can reach the all the configured hosts
- Check if the configured credentials have all the needed permissions on CassandraDB
