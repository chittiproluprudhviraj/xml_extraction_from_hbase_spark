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

object IMDMRecordParser {
  var properties = scala.collection.immutable.Map[String, String]()
  
  def main(args: Array[String]) {
    
      val sparkConf = new SparkConf().setAppName("IMDM Record Parser")
      val sc = new SparkContext(sparkConf)
      val propFilePath = args(0)
      val propFile = sc.textFile(propFilePath)
      properties = propFile.map(x => (x.split("=")(0), x.split("=")(1))).collect().toMap
      processIMDMGoldenSnapshot(sc : SparkContext)  
      
    }
  
    def processIMDMGoldenSnapshot (sc : SparkContext) = {
      
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE, properties("tableName"))
      conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, properties("columnFamily"))
      conf.set(TableInputFormat.SCAN_COLUMNS, properties("columns"))
      conf.set(TableInputFormat.SCAN_ROW_START, "1")
      conf.set(TableInputFormat.SCAN_ROW_STOP, "110900230") //100900230
      val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      val resultRDD = hbaseRDD.map(tuple => tuple._2)
      val keyValueRDD = resultRDD.map(r => (Bytes.toString(r.getValue(Bytes.toBytes("ri"), Bytes.toBytes("status"))),Bytes.toString(r.getValue(Bytes.toBytes("ri"), Bytes.toBytes("rawXml"))))) //.filter(f=> f._1 != "Deleted")
      println(keyValueRDD.count())
     /* val xmlrdd = keyValueRDD.map(mdmRec => mdmRec._2.toString)
      val xyz =  xmlrdd.collect()
      for(x <- xyz){
      val finalString =  parseIMDMGoldenXml(x)
       println(finalString)
      }*/
      
      val flattenedMDMRecords = keyValueRDD.map(mdmRec => parseIMDMGoldenXml(mdmRec._2.toString))
      flattenedMDMRecords.saveAsTextFile(properties("outPutPath"))
      
   
      
  }

  def parseIMDMGoldenXml(in : String) : String = {
    
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

  
    val xml = scala.xml.XML.loadString(in)
    val oEntity = xml \\ "EntityId"
    if (oEntity != null && oEntity.length >0 ) {
      entity_id = oEntity(0).text
    }
    
      //populate name
    for( name0 <- (xml \\ "TCRMPersonNameBObj") ) {
      val name1 = name0.child
      var nameType = ""
      for (n <- name1) {
        if (n.label == "NameUsageValue") {
          nameType = n.text
        }
        if (nameType == "Preferred") {
            if (n.label == "GivenNameOne") {
              first_name = n.text
            }
            if (n.label == "GivenNameTwo") {
              middle_name = n.text
            }
            if (n.label == "LastName") {
              last_name = n.text
            }
        }
      }
    }
    birth_date = "2999-01-01"
    val oBirth = xml \\ "BirthDate"
    if (oBirth != null && oBirth.length >0 ) {
      birth_date = oBirth(0).text
      if (birth_date.length >10){
        birth_date = birth_date.substring(0,10)
      }
      birth_date.filter(!":,".contains(_))
    }
    if (birth_date.length!=10)
      birth_date = "2999-01-01"
    
    
      //populate SSN
    for( ind0 <- (xml \\ "TCRMPartyIdentificationBObj") ) {
      val ind1 = ind0.child
      var identificationType = ""
      for (n <- ind1) {
        if (n.label == "IdentificationValue") {
         identificationType = n.text
        }
        if (n.label == "IdentificationNumber" && identificationType == "Social Security Number") {
         ssn = n.text
        }
      }
    }
      
    //populate address
    for( addr0 <- (xml \\ "TCRMPartyAddressBObj")) {
      val addr1 = addr0.child
      var addressType = ""
      for ( curAdr <- addr1) {
        if (curAdr.label == "AddressUsageValue") {
          addressType = curAdr.text
        }
        if (curAdr.label == "TCRMAddressBObj" && addressType == "Mailing") {
          for (c <- curAdr.child) {
            c.label match {
              case "AddressLineOne" => address_line_1 = c.text
              case "AddressLineTwo" => address_line_2 = c.text
              case "City" => city = c.text
              case "ZipPostalCode" => zip = c.text
              case "ProvinceStateValue" => state = c.text
              case "CountyCode" => county = c.text
              case "CountryValue" => country = c.text
              case "LatitudeDegrees" => latitude = c.text
              case "LongitudeDegrees" => longitude = c.text
              case _ =>
            }
          }
        }
      }
    }
  
    //populate SSN
    for( ind0 <- (xml \\ "TCRMAdminContEquivBObj") ) {
      val ind1 = ind0.child
      var adminSysValue = ""
      var adminSysCode = ""
      var keyChain=""
      for (n <- ind1) {
        if (n.label == "AdminSystemValue") {
         adminSysValue = n.text
        }
        if (n.label == "AdminPartyId") {
         adminSysCode = n.text
        }
        keyChain = adminSysValue+":"+adminSysCode+","
      }
      key_chain_temp += keyChain
    }
    if(key_chain_temp.length()>3){
          Key_chain += key_chain_temp.substring(0, key_chain_temp.length() - 1)
    }
    //val iMdmCols = Array(entity_id,first_name,middle_name,last_name,birth_date,ssn,address_line_1,address_line_2,city,zip,state,county,country,latitude,longitude,Key_chain.toString())
    val iMdmCols = Array(entity_id,first_name,middle_name,last_name,birth_date,ssn)
    
    return iMdmCols.mkString("|")
  
  }

}
