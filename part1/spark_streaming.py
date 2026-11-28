from pyspark.sql import SparkSession
from pyspark.sql import Column, DataFrame, functions as F

class SparkStreaming:
    def __init__(self):
        self.spark: SparkSession = None

    def get_spark_session(self) -> SparkSession:
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("spark-processing") \
                .config(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,com.mysql:mysql-connector-j:8.0.33")\
                .getOrCreate()
        return self.spark
    
    def _spark_clear_nondigit(self, df: DataFrame, columns: list) -> DataFrame:
        """
        Remove rows with non-numeric values in specified columns from the DataFrame.
        """
        for column in columns:
            numeric_value = self._numeric_value(df[column])
            df = df.filter(numeric_value.isNotNull() & ~F.isnan(numeric_value))
        return df

    def _numeric_value(self, column: Column) -> Column:
        return F.trim(column.cast("string")).try_cast("double")
    
    def _spark_clear(self, df: DataFrame, columns: list) -> DataFrame:
        """
        Remove rows with None values from the DataFrame.
        """
        df = self._spark_clear_nondigit(df, columns)
        return df.dropna(subset=columns)  
    
    def _spark_join_inner(self, df1: DataFrame, df2: DataFrame, sorted_column) -> DataFrame:
        """
        Union two DataFrames.
        """
        return df1.join(df2, on=sorted_column, how="inner").orderBy(sorted_column)
    
    def _spark_avg(self, df: DataFrame, group_by_column: str, avg_column: str) -> DataFrame:
        """
        Calculate the average of a specified column grouped by another column.
        """
        df = df.withColumn(avg_column, self._numeric_value(df[avg_column]))
        df = df.filter(df[avg_column].isNotNull() & ~F.isnan(df[avg_column]))
        return df.groupBy(group_by_column).avg(avg_column)