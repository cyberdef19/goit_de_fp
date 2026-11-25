
from pyspark.sql import DataFrame, SparkSession
from abc import ABC

class MySQLOperation(ABC):
    
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
    
    @property
    def _spark(self)-> SparkSession:
        return self._spark
    
    @_spark.setter
    def _spark(self, value: SparkSession):
        if not isinstance(value, SparkSession):
            return
        self._spark = value
    
    @property
    def _jdbc_url(self)-> str:
        return self._jdbc_url
    
    @_jdbc_url.setter
    def _jdbc_url(self, value: str):
        if not isinstance(value, str):
            return 
        self._jdbc_url = value

    @property
    def _jdbc_table(self)-> str:
        return self._jdbc_table

    @_jdbc_table.setter
    def _jdbc_table(self, value: str):
        if not isinstance(value, str):
            return
        self._jdbc_table = value

    @property
    def _username(self)-> str:
        return self._username

    @_username.setter
    def _username(self, value: str):
        if not isinstance(value, str):
            return
        self._username = value

    @property
    def _password(self) -> str:
        return self._password

    @_password.setter
    def _password(self, value: str):
        if not isinstance(value, str):
            return
        self._password = value
    
    def _spark_read(self)-> DataFrame:
          ...
    def _spark_write(self, df: DataFrame, mode: str = "append")-> None:
          ...

class MySQLReader(MySQLOperation):
    def _spark_read(self)-> DataFrame:
            return self._spark.read.\
                   format(self._format).\
                   option("url", self._jdbc_url).\
                   option("dbtable", self._jdbc_table).\
                   option("user", self._username).\
                   option("password", self._password).\
                   option("driver", "com.mysql.cj.jdbc.Driver").\
                   load()
        
    def _spark_write(self, df: DataFrame, mode: str = "append")-> None:
        raise NotImplementedError("Write operation is not supported in MySQLReader class.")
    
    
class MySQLWriter(MySQLOperation):
    def _spark_read(self)-> DataFrame:
        raise NotImplementedError("Read operation is not supported in MySQLWriter class.")
    
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


        