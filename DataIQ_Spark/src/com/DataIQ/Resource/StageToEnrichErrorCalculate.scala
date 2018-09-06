package com.DataIQ.Resource

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.sql.Timestamp
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

class StageToEnrichErrorCalculate extends java.io.Serializable {

  val Prop: Property = new Property()

  def CalculateError(FileName: String, Error_Col: String, Harmonic_ErrorDF: DataFrame, sqlContext: SQLContext, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem, flag: String): DataFrame =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      var ErrorDescription = ""
      val DataSetName = FileName.replaceAll(".csv", "")
      if (flag.equals("harmonic")) {
        ErrorDescription = "Columns: " + Error_Col + " is not proper(null/blank/0/Unparceable)"
      }
      else if (flag.equals("duplicate")) {
        ErrorDescription = "Columns: " + Error_Col + " is Duplicate in data"
      }
      else {
        /*//ErrorDescription = "Columns: " + Error_Col + " Referential Integrity Failed"
        val Crunched_rdd = Harmonic_ErrorDF.rdd.map(t => Row(t.mkString(","),t.get((t.length-1))))
        val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true),StructField("Error_Description", StringType, true)))
        val ErrorRecord = sqlContext.createDataFrame(Crunched_rdd, Error_Schema)
        val InsertTime = CT.CurrentTime()
       // val Error_Records = ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
        val Error_Records = ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
        return Error_Records*/
        ErrorDescription = "Column: " + Error_Col + " Referential Validation Failed"
      }
      val Crunched_rdd = Harmonic_ErrorDF.rdd.map(t => Row(t.mkString(",")))
      val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true)))
      val ErrorRecord = sqlContext.createDataFrame(Crunched_rdd, Error_Schema)
      val InsertTime = CT.CurrentTime()
      val Error_Records = ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
      return Error_Records
    }

  def CalculateRecord(FileName: String, Error_Stage_File: String, Status: String, Records_recieved_count: String, Records_rejected_count: String, Records_processed_count: String, StartTime: String, EndTime: String, sc: SparkContext, sqlContext: SQLContext, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem): Unit =
    {
      val ErrorPathMaster = adl_path + Prop.getProperty("Stage_to_Enrich_ErrorMaster") + "/date/" + Error_Stage_File

      val DataSetName = FileName.replaceAll(".csv", "")
      val Table_Schema = StructType(Array(StructField("Data_Set_Name", StringType, true)))
      val newRow = sc.parallelize(Seq(DataSetName)).map(t => Row(t))
      val DF = sqlContext.createDataFrame(newRow, Table_Schema)
      val Error_Records = DF.withColumn("Status", lit(Status)).withColumn("No_Of_records_received", lit(Records_recieved_count)).withColumn("No_Of_records_rejected", lit(Records_rejected_count)).withColumn("No_Of_records_processed", lit(Records_processed_count)).withColumn("Start_Time", lit(StartTime)).withColumn("EndTime", lit(EndTime))
      ErrorHandle(FileName, ErrorPathMaster, Error_Records, adl_path, sc, sqlContext, hdfs, hadoopConf)
    }

  def Error_Path(Error_Stage_File: String, resError: DataFrame, FileName: String, adl_path: String, sc: SparkContext, sqlContext: SQLContext, hdfs: FileSystem, hadoopConf: Configuration): Unit =
    {
      val ErrorPathRecord = adl_path + Prop.getProperty("Stage_to_Enrich_ErrorRecord") + "/date/" + Error_Stage_File
      ErrorHandle(FileName, ErrorPathRecord, resError, adl_path, sc, sqlContext, hdfs, hadoopConf)
    }

  def ErrorHandle(fileName: String, ErrorPath: String, ResDF: DataFrame, adl_path: String, sc: SparkContext, sqlContext: SQLContext, hdfs: FileSystem, hadoopConf: Configuration): Boolean =
    {
      var flag = false
      try {

        val CT: CurrentDateTime = new CurrentDateTime()
        val Date = CT.PresentDate()

        val Current_Timestamp = CT.CurrentDate()

        //val ErrorFolder = ErrorPath + "/" + Date
        val ErrorFolder = ErrorPath.replaceAll("date", Date)

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
        }
        ResDF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(ErrorFolder)

        val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/part*.csv"))
        var path_04 = listStatusReName(0).getPath()
        flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/" + fileName.replaceAll(".csv", "") + "_" + Current_Timestamp + ".csv"))
      } catch {

        case t: Throwable => {
          flag = false
        }
      }
      return flag
    }
}