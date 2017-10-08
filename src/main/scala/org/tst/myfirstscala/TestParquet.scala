package org.tst.myfirstscala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TestParquet {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("usage : spark-submit --class com.optum.uah.test.TestParquet --master yarn --deploy-mode cluster --queue uahgpdev_q1 uah_mergelayer-0.0.1-SNAPSHOT.jar <input_dir> <output_dir> ")
      return
    }
     val sparkSession = SparkSession.builder().appName("Convert2Parqet").enableHiveSupport().getOrCreate()
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val df = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "false") //reading the headers
        .option("delimiter", "|")
        .option("mode", "DROPMALFORMED")
        .load(args(0)); 
    println("Dataframe display")
    df.show(20)
    println("no of partitions :: "+df.rdd.getNumPartitions)
    df.write.mode("overwrite").parquet(args(1)+"/parquet")
  }
}