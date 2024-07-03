from pyspark.sql import SparkSession

def create_spark_session():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Vehicle Crash Analysis") \
        .getOrCreate()
    return spark