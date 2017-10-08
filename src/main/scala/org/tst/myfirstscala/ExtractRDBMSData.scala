package org.tst.myfirstscala

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, ResultSet}



object ExtractRDBMSData {
  val spark = SparkSession .builder().appName("Spark SQL basic example").enableHiveSupport().config("spark.sql.hive.convertMetastoreParquet", false).getOrCreate()
  def main(args: Array[String]) {
   /* if (args.length < 1) {
      println("Please pass the correct number of arguments")
      System.exit(0)
    }*/
     val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://dbsrp0686:3306").option("dbtable", "dlca01.opportunity").option("user", "dlca_own").option("password", "GqyuaQs0").option("numPartitions", 10).option("fetchsize", 30).load()
     println("Started the Extraction of the data")
     jdbcDF.createOrReplaceTempView("Opportunity")
     println(jdbcDF.schema)
     val oppbystatus = spark.sql("select STATUS_REASON_CD,count(*) from Opportunity group by STATUS_REASON_CD")
     oppbystatus.show()
     jdbcDF.write.parquet("/datalake/optum/optumrx/p_optum/prd/syn/developer/Opps") 
     println("Completed extracting the data")
     val count = jdbcDF.count()
     println("Number of records extracted - "+count)
/*     import spark.sql
     sql("CREATE EXTERNAL TABLE parquet_test LIKE Opportunity STORED AS PARQUET LOCATION '/datalake/optum/optumrx/p_optum/prd/syn/developer/opps_Text/';")*/
     
  }
}