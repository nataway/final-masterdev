
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Streaming {
    public static class saveFunction implements VoidFunction2<Dataset<Row>, Long> {

        @Override
        public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
            rowDataset.write()
                    .mode("append")
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://192.168.193.220/masterdev")
                    .option("user", "postgres")
                    .option("password", "postgres")
                    .option("dbtable","benhtim.test1")
                    .option("driver","org.postgresql.Driver")
                    .save();
        }
    }

    public static void main(String[] args) throws StreamingQueryException, InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Kafka Integration using Structured Streaming chibm")
                .master("local")
                .getOrCreate();


        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.193.220:9092")
                .option("subscribe", "test_final1")
                .option("group.id","group1")
                .option("startingOffsets","earliest")
                .load();
        UserDefinedFunction strLen = udf(
                ( Integer x) -> x/365,
                DataTypes.IntegerType);

        spark.udf().register("strLen", strLen);

        Dataset<Row> df1 = df.selectExpr("CAST(value AS STRING)", "timestamp as etl_time")
                .select(col("etl_time"),
                        split(col("value"),",").getItem(1).cast("int").alias("age"),
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
                        split(col("value"),",").getItem(12).cast("int").alias("cardio"));

        Dataset<Row> df2 = df1.selectExpr("etl_time","strLen(age) as age","gender","height",
                "weight","ap_hi","ap_lo","cholesterol","gluc","smoke","alco","active","cardio" );

        df2.printSchema();
//        df2.printSchema();
//        df2.show();


        df2.writeStream()
                .outputMode("append")
                .foreachBatch(saveFunction.class.newInstance())
                .option("checkpointLocation", "chibm/checkpoint1")
                .trigger(Trigger.ProcessingTime(1000))
                .start();
        spark.streams().awaitAnyTermination();
//        spark.sparkContext().stop();


    }



}
//id,age,gender,height,weight,ap_hi,ap_lo,cholesterol,gluc,smoke,alco,active,cardio