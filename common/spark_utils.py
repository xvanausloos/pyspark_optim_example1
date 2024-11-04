from pyspark.sql import SparkSession


def get_spark_session(app_name="ldi"):
    try:
        # Try to get the existing Spark session
        spark = SparkSession.builder.getOrCreate()
    except Exception as e:
        print("Error getting existing Spark session:", e)
        spark = SparkSession.builder.appName(app_name).getOrCreate()

    return spark
