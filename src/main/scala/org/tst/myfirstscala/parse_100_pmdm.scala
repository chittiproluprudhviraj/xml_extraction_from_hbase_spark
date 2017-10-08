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

object parse_100_pmdm {
  var properties = scala.collection.immutable.Map[String, String]()
  
  def main(args: Array[String]) {
    
      val sparkConf = new SparkConf().setAppName("PMDM Record Parser")
      val sc = new SparkContext(sparkConf)

      processPMDMGoldenSnapshot(sc)  
      
    }
  
    def processPMDMGoldenSnapshot (sc : SparkContext) = {
      
      val value_RDD = sc.textFile("/datalake/optum/optuminsight/udw/prd/pae/developer/npasapal/sample_pmdm.xml")
    
     /* val xmlrdd = keyValueRDD.map(mdmRec => mdmRec._2.toString)
      val xyz =  xmlrdd.collect()
      for(x <- xyz){
      val finalString =  parseIMDMGoldenXml(x)
       println(finalString)
      }*/
      
      val flattenedMDMRecords = value_RDD.map(mdmRec => parsePMDMGoldenXml(mdmRec.trim()))
      flattenedMDMRecords.saveAsTextFile("/datalake/optum/optuminsight/udw/prd/pae/developer/npasapal/ara_dm/test")
      
   
      
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

  
    val xml = scala.xml.XML.loadString(in)
    val oEntity = xml\\ "msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:providerIDObj"\\"msg:enterpriseID"
    println(oEntity)
   // val oEntity = xml \\ "EntityId"
    if (oEntity != null && oEntity.length >0 ) {
      entity_id = oEntity(0).text
    }
    
      //populate name
    
     val oFirstName = xml\\"msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:professional"\\"demo:name"\\"demo:firstName"
   // val oFirstName = xml \\ "EntityId"
     println("firstname "+oFirstName) 
    if (oFirstName != null && oFirstName.length >0 ) {
      first_name = oFirstName(0).text
    }
    
     //populate middle name
    
     val oMiddleName = xml\\"msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:professional"\\"demo:name"\\"demo:middleName"
   // val oMiddleName = xml \\ "EntityId"
    if (oMiddleName != null && oMiddleName.length >0 ) {
      middle_name = oMiddleName(0).text
    }
     
         //populate last name
    
     val oLastName = xml\\"msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:professional"\\"demo:name"\\"demo:lastName"
   // val oMiddleName = xml \\ "EntityId"
    if (oLastName != null && oLastName.length >0 ) {
      last_name = oLastName(0).text
    }
     
              //populate birth date
    
     val oBirthDate = xml\\"msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:professional"\\"demo:dateOfBirth"
   // val oMiddleName = xml \\ "EntityId"
    if (oBirthDate != null && oBirthDate.length >0 ) {
      birth_date = oBirthDate(0).text
    }
     
     
     //populate ssn
     
     for (ssn_nestlist <- xml \\"msg:ProvMasterMsg"\\"msg:body"\\"msg:provider"\\"msg:professional"\\"msg:hCPRole"\\"pdemo:ServiceProviderIDObj"\\"pdemo:taxID")
     {
       val demo_node = ssn_nestlist.child
       var ssn_text = ""
       for (x <- demo_node)
       {
         
         if (x.label == "name")
         {
           ssn_text = x.text
         }
          if (x.label == "value" && ssn_text == "SSN") {
         ssn = x.text
        }
       }
       
     }
    /*for( name0 <- (xml \\ "TCRMPersonNameBObj") ) {
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
    }*/
   /* birth_date = "2999-01-01"
    val oBirth = xml \\ "BirthDate"
    if (oBirth != null && oBirth.length >0 ) {
      birth_date = oBirth(0).text
      if (birth_date.length >10){
        birth_date = birth_date.substring(0,10)
      }
      birth_date.filter(!":,".contains(_))
    }
    if (birth_date.length!=10)
      birth_date = "2999-01-01" */
    
    
      //populate SSN
  /*  for( ind0 <- (xml \\ "TCRMPartyIdentificationBObj") ) {
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
    } */
      
 /*   //populate address
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
    } */
  
    //populate SSN
 /*   for( ind0 <- (xml \\ "TCRMAdminContEquivBObj") ) {
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
    }*/
   /* if(key_chain_temp.length()>3){
          Key_chain += key_chain_temp.substring(0, key_chain_temp.length() - 1)
    }*/
    //val iMdmCols = Array(entity_id,first_name,middle_name,last_name,birth_date,ssn,address_line_1,address_line_2,city,zip,state,county,country,latitude,longitude,Key_chain.toString())
    val iMdmCols = Array(entity_id,first_name,middle_name,last_name,birth_date,ssn)
    
    return iMdmCols.mkString("|")
  
  }

}
