from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class SecretManager:

    SCOPE = "macroeconomy"

    @staticmethod
    def get(secret_name):
        return dbutils.secrets.get(
            scope=SecretManager.SCOPE,
            key=secret_name
        )