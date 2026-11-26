
from typing import Protocol
from pyspark.sql import DataFrame


class ServiceReader(Protocol):
    def _spark_read(self)-> DataFrame:
        ...

class ServiceWriter(Protocol):
    def _spark_write(self, df: DataFrame, mode: str = "append")-> None:
              ...