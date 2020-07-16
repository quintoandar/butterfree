"""Holds common spark constants, present through all Butterfree."""

# from spark.sql.shuffle.partitions default value
DEFAULT_NUM_PARTITIONS = 200

# ratio between number of partitions per processor recommended (lower bound: 2)
# refs:
# https://github.com/vaquarkhan/Apache-Kafka-poc-and-notes/wiki/Apache-Spark-Join-guidelines-and-Performance-tuning
PARTITION_PROCESSOR_RATIO = 4
