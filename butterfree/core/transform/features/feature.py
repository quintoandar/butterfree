"""Feature entity."""
import warnings
from typing import List

from parameters_validation import non_blank
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.transformations import TransformComponent


class Feature:
    """Defines a Feature.

    A Feature is the result of a transformation over one (or more) data columns
    over an input dataframe. Transformations can be as simple as renaming,
    casting types, mathematical expressions or complex functions/models.

    Attributes:
        name: feature name.
            Can be use by the transformation to derive multiple output columns.
        description: brief explanation regarding the feature.
        dtype: data type for the output columns of this feature.
        from_column: original column to build feature.
            Used when there is transformation or the transformation has no
            reference about the column to use for.
        transformation: transformation that will be applied to create this
            feature.

    """

    def __init__(
        self,
        name: non_blank(str),
        description: non_blank(str),
        dtype: non_blank(DataType),
        from_column: non_blank(str) = None,
        transformation: non_blank(TransformComponent) = None,
    ) -> None:
        self.name = name
        self.description = description
        self.dtype = dtype
        self.from_column = from_column
        self.transformation = transformation

    @property
    def transformation(self) -> TransformComponent:
        """Attribute transformation getter.

        Returns
            A transformation for this feature.
        """
        return self._transformation

    @transformation.setter
    def transformation(self, transformation: TransformComponent) -> None:
        self._transformation = transformation
        if transformation is not None:
            self._transformation.parent = self

    def get_output_columns(self) -> List[str]:
        """Get output columns that will be generated by this feature engineering.

        Returns
            Output columns names.
        """
        if self.transformation is not None:
            return self.transformation.output_columns
        return [self.name]

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe for the transformation.

        Returns:
            Transformed dataframe.
        """
        if self.transformation:
            return self.transformation.transform(dataframe)

        if self.from_column:
            if self.name in dataframe.columns:
                warnings.warn(
                    f"The column name {self.name} "
                    "already exists in the dataframe and "
                    "will be overwritten with another column."
                )

            dataframe = dataframe.withColumn(self.name, col(self.from_column))

        if self.dtype:
            dataframe = dataframe.withColumn(
                self.name, col(self.name).cast(self.dtype.spark)
            )
        return dataframe
