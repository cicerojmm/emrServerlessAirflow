from pyspark.sql import SparkSession


def create_spark_session(app_name='pyspark-seed'):
    spark_builder = SparkSession.builder.appName(app_name)
    spark_session = spark_builder.getOrCreate()
    return spark_session
