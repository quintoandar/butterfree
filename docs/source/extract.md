# Source

Regarding the extract step, we can define a ```Source``` as a set of data sources in order to join your raw data for the transform step. 

Currently, we support three different data sources or, as it's called within ```Butterfree```, ```readers```:

* ```FileReader```: this reader loads data from a file, as the name suggests, and returns a dataframe. It can be instantiated as:

```python
file_reader = FileReader(
                id="file_reader_id",
                path="data_path",
                format="json"
              )
```

* ```TableReader```: this reader loads data from a table registered in spark metastore and returns a dataframe. It can be instantiated as:

```python
table_reader = TableReader(
                    id="table_reader_id",
                    database="table_reader_db",
                    table="table_reader_table"
               )
```

* ```KafkaReader```: this reader loads data from a kafka topic and returns a dataframe. It can be instantiated as:

```python
kafka_reader = KafkaReader(
                id="kafka_reader_id",
                topic="topic",
                value_schema=value_schema
                connection_string="host1:port,host2:port",
               )
```

After defining all your data sources, it's important to write a query in order to define the relation between them, something like this:

```python
source = Source(
   readers=[
       TableReader(
           id="table_reader_id",
           database="table_reader_db",
           table="table_reader_table",
       ),
       FileReader(id="file_reader_id", path="data_sample_path", format="json"),
   ],
   query=f"select a.*, b.feature2 "
   f"from table_reader_id a "
   f"inner join file_reader_id b on a.id = b.id ",
)
```

It's important to state that we have some pre-processing methods as well, such as filter and pivot. Feel free to check them [here](https://github.com/quintoandar/butterfree/tree/master/butterfree/extract/pre_processing).