from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_json, struct, col
import pyspark.sql.functions as F
try:
    from .service import ServiceReader, ServiceWriter
except ImportError:
    from service import ServiceReader, ServiceWriter

class KafkaOperation:
    def __init__(self, 
                 spark: SparkSession,
                 bootstrap_servers: str,
                 topic: str,
                 username: str = None,
                 password: str = None,
                 security_protocol: str = None,
                 sasl_mechanism: str = None):
        
        self.format="kafka"
        self._spark = spark
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._username = username
        self._password = password
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism

class KafkaReader(KafkaOperation, ServiceReader):
        def _spark_read(self) -> DataFrame:
            raw_df = self._spark.read.\
                   format(self.format).\
                   option("kafka.bootstrap_servers", self._bootstrap_servers).\
                   option("subscribe", self._topic).\
                   option("kafka.security.protocol", self._security_protocol).\
                   option("kafka.sasl.mechanism", self._sasl_mechanism).\
                   option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._username}" password="{self._password}";').\
                   load()
            return raw_df.select(F.from_json(F.col("value").cast("string"), self.schema).alias("data")).\
                   select("data.*")

class KafkaWriter(KafkaOperation, ServiceWriter):
        
        def _spark_write(self, df: DataFrame, mode: str = "append") -> None:
            # Підготовка DataFrame для запису в Kafka
            kafka_df = df.select(
            to_json(
                struct(*[col(c) for c in df.columns])
            ).alias("value")
            )
            kafka_df.write.\
               format(self.format).\
               option("kafka.bootstrap.servers", self._bootstrap_servers).\
               option("topic", self._topic).\
               option("kafka.security.protocol", self._security_protocol).\
               option("kafka.sasl.mechanism", self._sasl_mechanism).\
               option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._username}" password="{self._password}";').\
               mode(mode).\
               save()