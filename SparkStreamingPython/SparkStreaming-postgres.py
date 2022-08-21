from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, udf
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.config("spark.jars", "/home/chibm/postgresql-42.4.1.jar").appName("Structu").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "172.17.80.22:9092") \
  .option("subscribe", "test_final1") \
  .option("startingOffsets","earliest") \
  .load()

def strLen(x:int)->int:
    return x // 365
strLenUDF = udf(lambda z:strLen(z), IntegerType())
df1 = df.selectExpr("CAST(value AS STRING)", "timestamp as etl_time") \
                .select(col("etl_time"),
                        strLenUDF(split(col("value"),",").getItem(1).cast("int")).alias("age"),
                        split(col("value"),",").getItem(2).cast("int").alias("gender"),
                        split(col("value"),",").getItem(3).cast("int").alias("height"),
                        split(col("value"),",").getItem(4).cast("float").alias("weight"),
                        split(col("value"),",").getItem(5).cast("int").alias("ap_hi"),
                        split(col("value"),",").getItem(6).cast("int").alias("ap_lo"),
                        split(col("value"),",").getItem(7).cast("int").alias("cholesterol"),
                        split(col("value"),",").getItem(8).cast("int").alias("gluc"),
                        split(col("value"),",").getItem(9).cast("int").alias("smoke"),
                        split(col("value"),",").getItem(10).cast("int").alias("alco"),
                        split(col("value"),",").getItem(11).cast("int").alias("active"),
                        split(col("value"),",").getItem(12).cast("int").alias("cardio"),
                        to_date(split(col("value"),",").getItem(13),"yyyy-mm-dd").alias("time"))

# df2 = df1.selectExpr("etl_time","strLenUDF(age) as age","gender","height", \
#                 "weight","ap_hi","ap_lo","cholesterol","gluc","smoke","alco","active","cardio","time" )
# df1.printSchema()
# df1.show()

url = "jdbc:postgresql://172.17.80.23/masterdev"
table = "benhtim.test1"


def process_row(row, epoch_id):
    row.write.format("jdbc")\
        .mode("append") \
        .option("url", url) \
        .option("driver", "org.postgresql.Driver").option("dbtable", table) \
        .option("user", "postgres").option("password", "postgres").save()


df1.writeStream \
    .outputMode("append") \
    .foreachBatch(process_row) \
    .option("checkpointLocation", "/home/chibm/checkpoint1") \
    .trigger(processingTime='1 seconds') \
    .start()
spark.streams.awaitAnyTermination(timeout=30)
spark.sparkContext.stop()
# id;age;gender;height;weight;ap_hi;ap_lo;cholesterol;gluc;smoke;alco;active;cardio


# id;age;gender;height;weight;ap_hi;ap_lo;cholesterol;gluc;smoke;alco;active;cardio
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.postgresql:postgresql:42.2.18 /home/chibm/spark_gtvt/streaming.py
