from pyspark.sql import DataFrame, SparkSession
from part1.service import ServiceReader, ServiceWriter

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
            return self._spark.read.\
                   format(self.format).\
                   option("kafka.bootstrap.servers", self._bootstrap_servers).\
                   option("subscribe", self._topic).\
                   option("kafka.security.protocol", self._security_protocol).\
                   option("kafka.sasl.mechanism", self._sasl_mechanism).\
                   option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._username}" password="{self._password}";').\
                   load()

class KafkaWriter(KafkaOperation, ServiceWriter):
        def _spark_write(self, df: DataFrame, mode: str = "append") -> None:
            df.write.\
               format(self.format).\
               option("kafka.bootstrap.servers", self._bootstrap_servers).\
               option("topic", self._topic).\
               option("kafka.security.protocol", self._security_protocol).\
               option("kafka.sasl.mechanism", self._sasl_mechanism).\
               option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self._username}" password="{self._password}";').\
               mode(mode).\
               save()