from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirflowSparkTest").getOrCreate()

data = [("Airflow", 1), ("Spark", 2), ("Test", 3)]
df = spark.createDataFrame(data, ["word", "count"])

df.show()

df.write.csv("/opt/spark/shared/output/result.csv", header=True, mode="overwrite")

spark.stop()
