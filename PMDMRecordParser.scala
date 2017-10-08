package org.tst.myfirstscala

import java.lang.String
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

object PMDMRecordParser {
  var properties = scala.collection.immutable.Map[String, String]()
  
  def main(args: Array[String]) {
    
      val sparkConf = new SparkConf().setAppName("PMDM Record Parser")
      val sc = new SparkContext(sparkConf)
      val propFilePath = args(0)
      val propFile = sc.textFile(propFilePath)
      properties = propFile.map(x => (x.split("=")(0), x.split("=")(1))).collect().toMap
      processPMDMGoldenSnapshot(sc)  
      
    }
  
    def processPMDMGoldenSnapshot (sc : SparkContext) = {
      
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE, "/datalake/uhclake/prd/p_hdfs/uhg/raw/standard_access/pmd/hbase/professional")
      conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "ri")
      
      conf.set(TableInputFormat.SCAN_COLUMNS, "ri:rawMsg")
      conf.set(TableInputFormat.SCAN_ROW_START, "1498845644822-00-000000000002")
      conf.set(TableInputFormat.SCAN_ROW_STOP, "1498845644822-00-000000000008")
      //conf.set(TableInputFormat.SCAN_TIMERANGE_START, "1498854214737")
      //conf.set(TableInputFormat.SCAN_TIMERANGE_END, "1498894214737")
      println ("tableName  : " + properties("tablename"))
      println ("cf  : " + properties("columnfamily"))
      println ("cn  : " + properties("columns"))
      val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      println("count : "+ hbaseRDD.count())
      val resultRDD = hbaseRDD.map(tuple => tuple._2)
      val keyValueRDD = resultRDD.map(r => (Bytes.toString(r.getValue(Bytes.toBytes("ri"), Bytes.toBytes("rawMsg")))))
      
      val flattenedMDMRecords = keyValueRDD.map(mdmRec => parsePMDMGoldenXml(mdmRec.toString))
      flattenedMDMRecords.saveAsTextFile(properties("outputpath"))
      
  }

  def parsePMDMGoldenXml(in : String) : String = {
    
    var entity_id = "NA"
    var first_name = ""
    var middle_name = ""
    var last_name = ""
    var birth_date = ""
    var ssn = ""
    var address_line_1 = ""
    var address_line_2 = ""
    var city = ""
    var zip = ""
    var state = ""
    var county = ""
    var country = ""
    var latitude = ""
    var longitude = ""
    var Key_chain = ""
    var key_chain_temp = ""
    var postalcode_part1 = ""
    var postalcode_part2 = ""

   
    val xml = scala.xml.XML.loadString(in)
    
    //populate enterprise ID
    val oEntity = xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "providerIDObj" \\ "enterpriseID" \\ "value"
    if (oEntity != null && oEntity.length >0 ) {
      println("1.......")
      entity_id = oEntity(0).text
    }
    
      //populate first name
     val oFirstName = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"name"\\"firstName"
    if (oFirstName != null && oFirstName.length >0 ) {
       println("2.......")
      first_name = oFirstName(0).text
    }
    
     //populate middle name
    val oMiddleName = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"name"\\"middleName"
    if (oMiddleName != null && oMiddleName.length >0 ) {
       println("3.......")
      middle_name = oMiddleName(0).text
    }
     
    //populate last name
     val oLastName = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"name"\\"lastName"
    if (oLastName != null && oLastName.length >0 ) {
       println("4.......")
      last_name = oLastName(0).text
    }
     
    //populate birth date
     val oBirthDate = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"dateOfBirth"
    if (oBirthDate != null && oBirthDate.length >0 ) {
       println("5.......")
      birth_date = oBirthDate(0).text
    }
     
     
     //populate ssn
     val oSSN = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"hCPIDObj"\\"ssn"\\"value"
    if (oSSN != null && oSSN.length >0 ) {
       
      ssn = oSSN(0).text
    }
 
     /*for( addr0 <- (xml \\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"hCPRole")) {
      val addr1 = addr0.child
      var addressType = ""
      for ( curAdr <- addr1) {
        if (curAdr.label == "serviceProviderPracticeLocation") {
          for (c <- curAdr.child) {
            c.label match {
              case "locationName" => address_line_1 = c.text
              case "city" => city = c.text
              case "state" => state = c.text
              case "countyCode" => county = c.text
              case _ =>
            }
          } 
        }
        
      }
    }*/
     
     for( addr0 <- (xml \\"ProvMasterMsg"\\"body"\\"provider"\\"professional"\\"hCPRole")) {
      val addr1 = addr0.child
      var addressType = ""
      for ( curAdr <- addr1) {
        if (curAdr.label == "serviceProviderPracticeLocation") {
          for (c <- curAdr.child) {
            c.label match {
              case "locationName" => address_line_1 = c.text
              case "city" => city = c.text
              case "state" => state = c.text
              case "countyCode" => county = c.text
              case _ =>
            }
          for(b <- c)
          {
            if(b.label == "postalCode") {
              for (a <- b.child)
                a.label match  {
                  case "part1" => postalcode_part1 = a.text
                  case "part2" => postalcode_part2 = a.text
                  case _ => 
                 }
               }
            if (b.label == "latLong") {
            	for (a <- b.child)
            		a.label match {
            			case "lat" => latitude = a.text
            			case "long" => longitude = a.text
            			case _ =>
            			}
            		}
            if (b.label == "country") {
            	for (a <- b.child)
            		a.label match {
            			case "code" => country = a.text
            			case _ =>
             }
            }
           }
         }  
       }
     }
    }
     
     
    val iMdmCols = Array(entity_id,first_name,middle_name,last_name,birth_date,ssn,address_line_1,city,postalcode_part1+("-")+postalcode_part2,state,county,country, latitude,longitude)
    return iMdmCols.mkString("|")
  
    
  }

}
