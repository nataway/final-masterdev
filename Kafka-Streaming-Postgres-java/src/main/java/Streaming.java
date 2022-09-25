
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;


import javax.xml.crypto.Data;

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
                    .option("dbtable","benhtim.test2")
                    .option("driver","org.postgresql.Driver")
                    .save();
        }
    }


    public static void main(String[] args) throws StreamingQueryException, InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSession
                .builder()
                .appName("chibm")
                .master("local[*]")
//                .config("spark.yarn.stagingDir","hdfs://172.17.80.21:9000/user/hadoop")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

//        Dataset<Row> dfDB = spark
//                .read()
//                .format("jdbc")
//                .option("url", "jdbc:postgresql://192.168.193.220/masterdev")
//                .option("user", "postgres")
//                .option("password", "postgres")
//                .option("dbtable","benhtim.test2")
//                .option("driver","org.postgresql.Driver")
//                .load();
////        get maxID in postgres
//        int maxID = dfDB.select(max("id").cast("int").alias("maxID")).first().getInt(0);
//        System.out.println(maxID);

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.193.93:9092")
                .option("subscribe", "final1")
                .option("group.id","group3")
                .option("startingOffsets","earliest")
                .load();
        UserDefinedFunction strLen = udf(
                ( Integer x) -> x/365,
                DataTypes.IntegerType);

        spark.udf().register("strLen", strLen);

        Dataset<Row> df1 = df.selectExpr("CAST(value AS STRING)", "timestamp as etl_time")
                .select(col("etl_time"),
                        split(col("value"),",").getItem(0).cast("int").alias("id"),
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
                        split(col("value"),",").getItem(12).cast("int").alias("cardio"),
                        split(col("value"),",").getItem(13).cast("timestamp").alias("time"))
                        .selectExpr("etl_time","id","strLen(age) as age","gender","height",
                        "weight","ap_hi","ap_lo","cholesterol","gluc","smoke","alco","active","cardio","time");

        Dataset<Row> df3 = df1
                .withWatermark("etl_time", "0 minutes")
                .sort(
                        window(col("etl_time"), "48 hours", "48 hours"),
                        col("etl_time").desc()
                )
                .groupBy(
                        window(col("etl_time"), "48 hours", "48 hours"),
                        col("id"))
//                .count()
//                .sort(col("id").asc());
                .agg(first("etl_time").alias("etl_time"),
                        first("age").alias("age"),
                        first("gender").alias("gender"),
                        first("height").alias("height"),
                        first("weight").alias("weight"),
                        first("ap_hi").alias("ap_hi"),
                        first("ap_lo").alias("ap_lo"),
                        first("cholesterol").alias("cholesterol"),
                        first("gluc").alias("gluc"),
                        first("smoke").alias("smoke"),
                        first("alco").alias("alco"),
                        first("active").alias("active"),
                        first("cardio").alias("cardio"),
                        first("time").alias("time")
                        ).drop("window");
//              .save();





        df3.writeStream()
                .outputMode("complete")
//                .format("parquet")
//                .option("path", "chibm2/data_tracking1")
                .foreachBatch(saveFunction.class.newInstance())
                .option("checkpointLocation", "chibm1/checkpoin")  //hdfs:///user/chibm/checkpoint1
                .trigger(Trigger.ProcessingTime(1000))
                .start();
        spark.streams().awaitAnyTermination(60000);
        spark.sparkContext().stop();
//
//
    }



}
//id,age,gender,height,weight,ap_hi,ap_lo,cholesterol,gluc,smoke,alco,active,cardio

//    SELECT age,
//            CASE
//    WHEN gender = 1 THEN 'women'
//        ELSE 'men'
//        END gender,
//        height,
//        weight,
//        ap_hi as Systolic_blood_pressure,
//        CASE
//        WHEN gluc = 1 THEN 'normal'
//        WHEN gluc = 2 THEN 'above normal'
//        ELSE 'well above normal'
//        END cholesterol,
//        CASE
//        WHEN gluc = 1 THEN 'normal'
//        WHEN gluc = 2 THEN 'above normal'
//        ELSE 'well above normal'
//        END Glucose,
//        smoke,
//        alco,
//        active as Physical_activity,
//        cardio,
//        time
//        from benhtim.test1 ;