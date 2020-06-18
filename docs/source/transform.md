# Feature Sets

Regarding the transform step, we can define a ```FeatureSet``` as a set of features in order to apply the suggested transformations to your data.

It's possible to use three different kinds of features:

* ```KeyFeature```: a ```FeatureSet``` must contain one or more ```KeyFeatures```, which will be used as keys when storing the feature set dataframe as tables. The ```FeatureSet``` may validate keys that are unique for the latest state of a feature set.

* ```TimestampFeature```: a ```FeatureSet``` must contain one ```TimestampFeature```, which will be used as a time tag for the state of all features. By containing a timestamp feature, users may time travel over their features. The ```FeatureSet``` may validate that the set of keys and timestamp are unique for a feature set. By defining a timestamp column, the feature set will always contain a data column called "timestamp" of ```TimestampType``` (spark dtype).

* ```Feature```: a ```Feature``` is the result of a transformation over one (or more) data columns over an input dataframe. Transformations can be as simple as renaming, casting types, mathematical expressions or complex functions/models. It can be instantiated as:

```python
feature = Feature(
   name="new_feature",
   description="simple feature renaming",
   from_column="feature"
)
```

It's possible to define the desired transformation to be applied over your input data by defining the ```transformation``` parameter within the ```Feature``` instantiation. The following transform components can be used in ```Butterfree```:

* ```SparkFunctionTransform```: this component can bem used when a [pyspark sql function](https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#module-pyspark.sql.functions) needs to be used. It can be instantiated as:

```python
feature = Feature(
   name="feature",
   description="Spark function transform usage example",
   transformation=SparkFunctionTransform(
      functions=[Function(functions.cos, DataType.DOUBLE)])
)
```

Feel free to check more [here](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/transformations/spark_function_transform.py).

* ```SQLExpressionTransform```: as the name suggests, this component can be used when a user wants to define the feature's transformation using SQL. It can be instantiated as:

```python
feature = Feature(
   name="feature",
   description="SQL expression transform usage example",
   transformation=SQLExpressionTransform(expression="feature1/feature2"),
)
```

You can find more info [here](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/transformations/sql_expression_transform.py).

* ```CustomTransform```: this component can be used when an user wants to define a custom transformation by defining a method. For instance:

```python
def divide(df, parent_feature, column1, column2):
    name = parent_feature.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df

feature = Feature(
   name="feature",
   description="custom transform usage example",
   transformation=CustomTransform(
       transformer=divide, column1="feature1", column2="feature2",
   )
)
```

You can take a look at the code [here](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/transformations/custom_transform.py).

* ```H3HashTransform```: this component can be used when an user wants to convert latitude and longitude values into a hash. You can learn more about H3 [here](https://eng.uber.com/h3/). For instance:

```python
feature = Feature(
   name="feature",
   description="h3 hash transform usage example",
   transformation=H3HashTransform(
       h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
       lat_column="lat",
       lng_column="lng",
   )
)
```

You can read the code [here](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/transformations/h3_transform.py).

* ```StackTransform```: this component can be used when an user wants to create a stack column based on some given values. For instance:

```python
feature = Feature(
    name="stack_ids",
    description="id_a and id_b stacked in a single column.",
    transformation=StackTransform("id_a", "id_b"),
)
```

Feel free to check it out [here](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/transformations/stack_transform.py).

Finally, an example of a ```FeatureSet``` is provided:

```python
feature_set = FeatureSet(
   name="feature_set",
   entity="entity",
   description="description",
   features=[
       Feature(
           name="feature1",
           description="test",
           transformation=SparkFunctionTransform(
                functions=[
                           Function(F.avg, DataType.DOUBLE),
                           Function(F.stddev_pop, DataType.DOUBLE)]
            ).with_window(
                partition_by="id",
                order_by=TIMESTAMP_COLUMN,
                mode="fixed_windows",
                window_definition=["2 minutes", "15 minutes"],
            ),
       ),
       Feature(
           name="divided_feature",
           description="unit test",
           transformation=CustomTransform(
               transformer=divide, column1="feature1", column2="feature2",
           ),
       ),
   ],
   keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
   timestamp=TimestampFeature(),
)
```

# Aggregated Feature Sets

When an user desires to use aggregated features, an ```AggregatedFeatureSet``` can be used. Besides ```KeyFeature```, ```TimestampFeature``` and ```Feature``` classes, we're going to use a new component regarding the transformations, called ```AggregatedTransform```.

* ```AggregatedTransform```:  this transformation needs to be used within an ```AggregatedFeatureSet```. Unlike the other transformations, this class won't have a transform method implemented. The idea behind aggregating is that, in spark, we should execute all aggregation functions after in a single group by. So an ```AggregateFeatureSet``` will have many features with an associated ```AggregatedTransform```. If each one of them needs to apply a ```groupby.agg()```, then we must join all the results in the end, making this computation extremely slow. It's important to say that the ```AggregatedFeatureSet``` can be used with both time and row windows.

Some examples are provided below:

```python
feature_set = AggregatedFeatureSet(
   name="aggregated_feature_set",
   entity="entity",
   description="description",
   features=[
       Feature(
           name="feature1",
           description="test",
           transformation=AggregatedTransform(
                functions=[
                   Function(F.avg, DataType.DOUBLE),
                   Function(F.stddev_pop, DataType.DOUBLE)],
            ),
            from_column="rent",
       ),
   ],
   keys=[KeyFeature(name="id", description="some key")],
   timestamp=TimestampFeature(from_column="ts"),
)
```

You could also add a time window, like this:

```python
feature_set.with_windows(definitions=["3 days"])
```

Also, you can add a ```pivot``` operation:

```python
feature_set.with_pivot(
    column="status",
    values=["publicado", "despublicado", "alugado"]
).with_windows(definitions=["1 day", "2 weeks"])
```

Please refer to [this](https://github.com/quintoandar/butterfree/blob/staging/butterfree/core/transform/aggregated_feature_set.py) code in order to understand more. Also, check our [examples section](https://github.com/quintoandar/butterfree/tree/staging/examples).