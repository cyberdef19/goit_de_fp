
from pyspark.sql import functions as F
from mysql_utils import MySQLReader, MySQLWriter
from kafka_utils import KafkaReader, KafkaWriter
from spark_streaming import SparkStreaming
from configs import kafka_config, mysql_config

MAX_WEIGHT_KG = 300

spark_streaming = SparkStreaming()
spark = spark_streaming.get_spark_session()

#Зчитати дані з mysql
reader_athletes = MySQLReader(
    spark=spark,
    jdbc_url=mysql_config["jdbc_url"],
    jdbc_table=mysql_config["jdbc_table_athletes"],
    username=mysql_config["username"],
    password=mysql_config["password"]
)

reader_results = MySQLReader(
    spark=spark,
    jdbc_url=mysql_config["jdbc_url"],
    jdbc_table=mysql_config["jdbc_table_results"],
    username=mysql_config["username"],
    password=mysql_config["password"]
)
data_athletes = reader_athletes._spark_read()
data_athletes_cleaned = spark_streaming._spark_clear(data_athletes, ["weight", "height"])
data_athletes_cleaned = data_athletes_cleaned.filter(
    F.col("weight").try_cast("double").between(0, MAX_WEIGHT_KG)
)
data_results = reader_results._spark_read()

writer_athletes_kafka = KafkaWriter(
    spark=spark,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    topic=kafka_config["topic_athletes_results"],
    username=kafka_config["username"],
    password=kafka_config["password"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"]
)

reader_athletes_kafka = KafkaReader(
    spark=spark,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    topic=kafka_config["topic_athletes_results"],
    username=kafka_config["username"],
    password=kafka_config["password"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"]
) 

#writer_athletes_kafka._spark_write(data_results, mode="append")
#athletes_results = reader_athletes_kafka._spark_read()

df_joined = spark_streaming._spark_join_inner(data_athletes_cleaned, data_results, sorted_column="athlete_id")
spark_streaming._spark_avg(df_joined, group_by_column="medal", avg_column="weight").show()
    