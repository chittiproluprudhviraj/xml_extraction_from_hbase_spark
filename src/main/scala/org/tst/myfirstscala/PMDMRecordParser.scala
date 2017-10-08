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
  case class Address(organization_name : String, address_line_1 : String, city : String, zip : String, state : String,
                     county : String, country : String, latitude : String, longitude : String)
  case class Keychain(key_chain : String, key_value : String)

  case class PMdmCol(entity_id : String, first_name : String, middle_name  : String, last_name  : String, birth_date : String,
                     ssn : String, address : List[Address],key_chain : List[Keychain])
  
    def processPMDMGoldenSnapshot (sc : SparkContext) = {
      
     
      
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE, properties("tablename"))
      conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, properties("columnfamily"))
      conf.set(TableInputFormat.SCAN_COLUMNS, properties("columns"))
      val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      println("count : "+ hbaseRDD.count())
      val resultRDD = hbaseRDD.map(tuple => tuple._2)
      val keyValueRDD = resultRDD.map(r => (Bytes.toString(r.getValue(Bytes.toBytes("ri"), Bytes.toBytes("rawMsg")))))
      
      val flattenedMDMRecords = keyValueRDD.map(mdmRec => parsePMDMGoldenXml(mdmRec.toString))
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      flattenedMDMRecords.toDF.write.format("parquet").save(properties("outputpath"))
      }

  def parsePMDMGoldenXml(in : String) : PMdmCol = {
    
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
    var key_chain_temp = ""
    var postalcode_part1 = ""
    var postalcode_part2 = ""
    var sourcename = ""
    var MPINsourcecode = ""
    var MPINeffdate = ""
    var PULSEsourcecode = ""
    var PULSEeffdate = ""
    var organization_name = ""
   
    val xml = scala.xml.XML.loadString(in)
    
    val professionalxml = xml\\"ProvMasterMsg"\\"body"\\"provider"\\"professional"
    //populate enterprise ID
    val oEntity = xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "providerIDObj" \\ "enterpriseID" \\ "value"
    if (oEntity != null && oEntity.length >0 ) {
      println("1.......")
      entity_id = oEntity(0).text
    }
    
      //populate first name
     val oFirstName = professionalxml\\"name"\\"firstName"
    if (oFirstName != null && oFirstName.length >0 ) {
       println("2.......")
      first_name = oFirstName(0).text
    }
    
     //populate middle name
    val oMiddleName = professionalxml\\"name"\\"middleName"
    if (oMiddleName != null && oMiddleName.length >0 ) {
       println("3.......")
      middle_name = oMiddleName(0).text
    }
     
    //populate last name
     val oLastName = professionalxml\\"name"\\"lastName"
    if (oLastName != null && oLastName.length >0 ) {
       println("4.......")
      last_name = oLastName(0).text
    }
     
    //populate birth date
     val oBirthDate = professionalxml\\"dateOfBirth"
    if (oBirthDate != null && oBirthDate.length >0 ) {
       println("5.......")
      birth_date = oBirthDate(0).text
    }
     
     
     //populate ssn
     val oSSN = professionalxml\\"hCPIDObj"\\"ssn"\\"value"
    if (oSSN != null && oSSN.length >0 ) {
       
      ssn = oSSN(0).text
    }
     
     var addrList:List[Address] = List()
     
     for( addr0 <- (professionalxml\\"hCPRole")) {
       val addr1 = addr0.child
       var addressType = ""
       address_line_1 = ""
       city = ""
       state = ""
       country = ""
       county = ""
       postalcode_part1 = ""
       postalcode_part2 = ""
       zip = ""
       latitude = ""
       longitude = ""

       for ( curAdr <- addr1) {
        if (curAdr.label == "ServiceProviderIDObj"){
          for(k <- curAdr.child){
            if(k.label == "relationshipOwner"){
              for (c <- k.child){
                if (c.label == "relatedSPOwner"){
                   for (b <- c.child){
                     if(b.label == "organizationNames"){
                         for (a <- b.child){
                              if(a.label == "name"){
                                organization_name = a.text
                                  }
                                }
                               }
                             }
                           }
                          }
                        }
                       }
                      }
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
                  case "part1" => postalcode_part1 =  a.text
                  case "part2" => postalcode_part2 =  a.text
                  case _ => 
                 }
              zip = postalcode_part1 + '-' + postalcode_part2
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
       val addr = Address(organization_name,address_line_1, city, zip, state, county, country, latitude, longitude)
       addrList =  addr :: addrList
      }
     
     var keyList:List[Keychain] = List()
     for ( keyc0 <- (professionalxml\\"hCPRole")){
       val keyc1 = keyc0.child
       for ( curAdr <- keyc1) {
       if(curAdr.label == "hCPRoleIDObj")
        {
         val othroles = curAdr.child
         for(othrole <- othroles){
              if(othrole.label=="OtherID"){
                var newcode = ""
                var neweffdate = ""
                for(othchild <- othrole.child){
                  if(othchild.label == "name" ){
                  sourcename = othchild.child.text
                  }
                    if(othchild.label == "value" ){
                      newcode = othchild.child.text
                      }
                      if(othchild.label == "EffDate" ){
                        for(effdates <- othchild.child){
                          if(effdates.label=="effStartDate"){
                            neweffdate=effdates.child.text
                            }
                           }
                          }
                      if(othchild.label == "EffDate" ){
                        if(sourcename=="MPIN") {
                          if (MPINeffdate == "" || neweffdate > MPINeffdate) {
                            MPINsourcecode = newcode
                            MPINeffdate = neweffdate
                            }
                            }else if (sourcename=="PULSE PROV CODE"){
                            if (PULSEeffdate == "" || neweffdate > PULSEeffdate) {
                              PULSEsourcecode = newcode
                              PULSEeffdate = neweffdate
                    }
                  }
                }
              }
            } 
          }  
         }
        }
     }
     
     var keychainList:List[Keychain] = List()
//     var key_chain = ""
if(MPINsourcecode != ""){
  val key_chain = Keychain("NDB",MPINsourcecode)
  keychainList = key_chain :: keychainList
//  key_chain = "NDB:"+MPINsourcecode
}
    if(PULSEsourcecode != ""){
      val key_chain = Keychain("PULSE",PULSEsourcecode)
      keychainList = key_chain :: keychainList
      //    key_chain += ",PULSE:"+PULSEsourcecode
      }
//    }else if(PULSEsourcecode != ""){
//        val key_chain = Keychain("NDB",MPINsourcecode)
//        keychainList = key_chain :: keychainList
//    }
//    key_chain = "PULSE:"+PULSEsourcecode
//    
//    val keychain = Keychain(key_chain)
//    keyList =  keychain :: keyList
    
    

    return PMdmCol(entity_id,first_name,middle_name,last_name,birth_date,ssn,addrList,keychainList)
      
    
  } 

}