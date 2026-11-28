
from pyspark.sql import DataFrame, SparkSession
try:
    from .service import ServiceReader, ServiceWriter
except ImportError:
    from service import ServiceReader, ServiceWriter

class MySQLOperation:
    
    def __init__(self, 
                 spark: SparkSession,
                 jdbc_url: str = None, 
                 jdbc_table: str = None, 
                 username: str = None, 
                 password: str = None,
                 ):
        
        self._format = "jdbc"
        self._jdbc_url = jdbc_url
        self._jdbc_table = jdbc_table
        self._username = username
        self._password = password
        self._spark = spark
    
    

class MySQLReader(MySQLOperation, ServiceReader):
    def _spark_read(self)-> DataFrame:
            return self._spark.read.\
                   format(self._format).\
                   option("url", self._jdbc_url).\
                   option("dbtable", self._jdbc_table).\
                   option("user", self._username).\
                   option("password", self._password).\
                   option("driver", "com.mysql.cj.jdbc.Driver").\
                   load()
    
    
class MySQLWriter(MySQLOperation, ServiceWriter):
    
    def _spark_write(self, df: DataFrame, mode: str = "append")-> None:
        df.write.\
           format(self._format).\
           option("url", self._jdbc_url).\
           option("dbtable", self._jdbc_table).\
           option("user", self._username).\
           option("password", self._password).\
           option("driver", "com.mysql.cj.jdbc.Driver").\
           mode(mode).\
           save()         


        