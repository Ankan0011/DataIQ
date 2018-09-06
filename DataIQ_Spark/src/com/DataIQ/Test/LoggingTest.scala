package com.DataIQ.Test

import java.util.logging.Logger
	import java.util.logging.Handler
	import java.util.logging.FileHandler
	import java.util.logging.Level
	import org.joda.time.format.DateTimeFormat
	import org.apache.spark.sql.functions._
	import org.apache.spark.sql.SQLContext
	import org.apache.spark.sql.Row
	import com.DataIQ.Resource.CurrentDateTime
	import org.apache.spark.sql.types.StructField
	import org.apache.spark.sql.types.StringType
	import org.apache.spark.sql.types.StructType
	import org.apache.spark.sql.types.LongType
	import java.text.SimpleDateFormat
	import java.util.Date

class LoggingTest extends java.io.Serializable{
  /*val log:Logger = Logger.getLogger("LoggingTest")
  val fileHandler:Handler = new FileHandler("adl://bienodad56872stgadlstemp.azuredatalakestore.net/Backup/Log/Test.txt")
  def TestLogging()
  {
    log.addHandler(fileHandler)
    fileHandler.setLevel(Level.ALL)
    log.setLevel(Level.ALL)
    try {
      log.log(Level.FINE,"Inside Try")
      val a = 2/0
    } catch {
      case t: Throwable => {
        log.log(Level.SEVERE,t.getMessage)
      }
    }
  }*/
  def To_Milli(date:String, format_temp:String):Long = 
  {
    var res_op = 0L
    try {
      val format: SimpleDateFormat = new SimpleDateFormat(format_temp) 
      res_op = format.parse(date).getTime
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    return res_op
  }
  def Long_To_year(time:Long)
  {
    var res_op = ""
    try {
      val format: SimpleDateFormat = new SimpleDateFormat("yyyy")
    val Date_Temp:Date = new Date(time)
    res_op = format.format(Date_Temp)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    return res_op
  }
  def testDate(sqlContext:SQLContext){
    val CT: CurrentDateTime = new CurrentDateTime()
    
    val schemaString = "TransWeek,LocationNbr,ProductNbr,ClearanceSales,ClearanceUnits,CompSales,CompUnits,PromoSales,PromoUnits,RegularSales,RegularUnits,TotalSales,TotalUnits,CircularSales,CircularUnits,DiscontinuedSales,DiscontinuedUnits,TCPSales,TCPUnits"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema_temp = StructType(fields)
    val schema = schema_temp.add(StructField("Milli_Count", LongType, nullable = true))
        
    val Enrich_File = "adl://bienodad56872stgadls.azuredatalakestore.net/Raw/Backup/Test/TGT_POS_20170719.csv"

    val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").load(Enrich_File)
    
    val Transform_Date_rdd = Enrich_DF.rdd.map(t => Row(CT.day(t.get(0).toString(), "yyyy-MM-dd"), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18),To_Milli(t.get(0).toString(),"yyyy-MM-dd")))
    val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, schema)

    //val To_Milli = udf( (x: String) =>(formatter.parseDateTime(x).getMillis))

    //val Enrich_Milli_DF = Enrich_DF.withColumn("Milli_Count", To_Milli(col("TransWeek")))
  
   
    val Min_Max_DF = Clean_DF_temp.agg(min(col("Milli_Count")),max(col("Milli_Count")))
    
    val min_Date = Min_Max_DF.collect().apply(0).getLong(0)
    val max_Date = Min_Max_DF.collect().apply(0).getLong(1)
    
    val year_interval = 52L*604800000L
    var temp_millisecond:Long = min_Date
    
    while (temp_millisecond<max_Date) {
      val from_date = temp_millisecond
      val to_date = from_date+year_interval
      val year = Long_To_year(from_date)
      
      val Interval_DF = Clean_DF_temp.where(Clean_DF_temp("Milli_Count")>= from_date && Clean_DF_temp("Milli_Count")< to_date)
      val Enrich_write_DF = Interval_DF.drop("Milli_Count")
      val Count_DF = Enrich_write_DF.count()
      if(Count_DF>0L)
      {
        Enrich_write_DF.repartition(1).write.option("header", "true").mode("append").csv("adl://bienodad56872stgadls.azuredatalakestore.net/Raw/Backup/Test/"+year)
      }
      temp_millisecond = temp_millisecond+year_interval
    }
    //Min_Max_DF.repartition(1).write.option("header", "true").mode("append").csv("adl://bienodad56872stgadls.azuredatalakestore.net/Raw/Backup/Test/Res.csv")

  }
  
}