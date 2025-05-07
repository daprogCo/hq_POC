from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

data = [("Airflow", 1), ("Spark", 2), ("Test", 3)]
df = spark.createDataFrame(data, ["word", "value"])

df.show()

spark.stop()
