package com.DataIQ.StageToEnrichProcessCalculate

import java.sql.Timestamp
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.SparkContext
import java.util.Date
import org.apache.hadoop.conf.Configuration
import java.net.URI
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite
import com.DataIQ.Resource.StageToEnrichErrorCalculate

class WalmartPOS_USASCC extends java.io.Serializable {
  def PresentDate(): String =
    {
      val d: Timestamp = new Timestamp(System.currentTimeMillis())
      val formattter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS")
      val transfer_format: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val TimeStamp = formattter.parse(d.toString())
      val day = transfer_format.format(TimeStamp)
      return day
    }

  def day(t1: String, Format: String): String =
    {
      /**
       * This method will convert the date format
       * input format is 'yyyy-MM-dd' and output format is 'MM/dd/yyyy'
       * input parameter:- t1: String
       * output parameter:- day:String
       */
      var day = ""
      try {
        val format: SimpleDateFormat = new SimpleDateFormat(Format) //Input format
        val formatter: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy") //Output format
        val d = format.parse(t1)
        day = formatter.format(d)

      } catch {
        case t: Throwable => { //In case of exception 
          if (t.toString().length() > 0) {
            day = "Unparceable"
            return day
          }
        }
      }
      return day.toString()
    }
  def day_Compare(t1: String, t2: String): Int =
    {
      var day: Int = 0
      try {
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val format2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val d1: Date = format.parse(t1)
        val d2: Date = format2.parse(t2)
        day = d1.compareTo(d2)

      } catch {
        case t: Throwable => {
          if (t.toString().length() > 0) {

          }
        }
      }
      return day
    }

  def To_Milli(date: String, format_temp: String): Long =
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
  def To_Milli_year(date: String, format_temp: String): Int =
    {
      var year = 0
      try {
        val format: SimpleDateFormat = new SimpleDateFormat(format_temp)
        val format_to: SimpleDateFormat = new SimpleDateFormat("yyyy")
        val date_new = format.parse(date)
        year = format_to.format(date_new).toInt
        //res_op = format.parse(date).getYear
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      return year
    }
  def IncrementWalmart(Stage_File: String, Enrich_File: String, Walmart_Item: String, Walmart_Store: String, schemaString: String, Enrich_File_Name: String, UpdatedFiles_Folder_Path: String, FieldName: String, adl_path: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, FolderPath_temp: String, ErrorPathRecord: String): Boolean =
    {
      var res_flag = false
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementWalmart \n")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = 0L
      val Error_Stage_File = Stage_File.replaceAll("/Stage", "").replaceAll(adl_path, "")
      try {
        import sqlContext.implicits._

        /**
         * Create Enrich Directory if not present
         */
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Enrich_File))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Enrich_File))
        }

        //Create the Schema from SchemaString
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        val schema_temp = schema.add(StructField("Milli_Count", LongType, nullable = true))

        //Load the TargetPOS_Product file into DataFrame, select only one column :- "PRDC_TAG" as we require only this column
        sb.append(CT.CurrentTime() + " : Master File(s) loading....\n Loading Master_Walmart_Item \n")
        val Master_walmartITEM_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(Walmart_Item).select("ITEM_KEY").distinct()

        //Load the TargetPOS_Location file into DataFrame, select only one column :- "MKT" as we require only this column
        sb.append(CT.CurrentTime() + " : Master File(s) loading....\n Loading Master_Walmart_Store \n")
        val Master_walmartSTORE_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(Walmart_Store).select("STORE_KEY").distinct()

        //Load the TargetPOS_Sales Stage file into DataFrame
        sb.append(CT.CurrentTime() + " : Master_Walmart_Store loaded \n Loading Stage_File_DF \n")
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File + "/*.csv")

        //Apply Date Transformation
        val Transform_Date_rdd = Stage_File_DF.rdd.map(t => Row(t.get(0), t.get(1),t.get(2), t.get(3),CT.day(t.getString(4), "yyyyMMdd"), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27),To_Milli(t.getString(4), "yyyyMMdd")))
        val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, schema_temp)

        //Clean DataFrame
        val Clean_DF = Clean_DF_temp.filter("ITEM_KEY != '' and STORE_KEY != '' and PERIOD_KEY != '' and PERIOD_KEY != 'Unparceable' and ITEM_KEY is not null and STORE_KEY is not null and PERIOD_KEY is not null and ITEM_KEY != '0' and STORE_KEY != '0' and PERIOD_KEY != '0'")

        var Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        try {
          sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n : Enrich dataframe reading started.... \n")
          val temp_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(Enrich_File + "/*.csv")
          Enrich_DF = Enrich_DF.union(temp_Enrich_DF)
        } catch {
          case t: Throwable =>
            {
              sb.append(CT.CurrentTime() + " : No file present in enriched folder, seems like first time processing of this POS data..... \n")
            }
        }

        sb.append(CT.CurrentTime() + " : Referential integrity validation started.... \n")
        /**
         * Following two lines are for refrential integrity
         */
        val STORE_Check = Clean_DF.as("D1").join(Master_walmartSTORE_DF.as("D2"), Clean_DF("STORE_KEY") === Master_walmartSTORE_DF("STORE_KEY")).select($"D1.*")
        val ITEM_Clean = STORE_Check.as("D3").join(Master_walmartITEM_DF.as("D4"), STORE_Check("ITEM_KEY") === Master_walmartITEM_DF("ITEM_KEY")).select($"D3.*")
        sb.append(CT.CurrentTime() + " : Referential integrity validation completed.... \n")
        /**
         * Following code is for date compare, ie to check if the file is incremental or restatement.
         */
        var dateCom_flag = ""

        val Temp_Enrich_rdd = Enrich_DF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27),To_Milli(t.getString(4), "MM/dd/yyyy")))

        val Temp_Enrich_DF = sqlContext.createDataFrame(Temp_Enrich_rdd, schema_temp)

        var stage_MIN_date = 0L
        val Stage_MIN_MAX = ITEM_Clean.select("Milli_Count").agg(min(col("Milli_Count")), max(col("Milli_Count"))).collect().apply(0)
        try {
          stage_MIN_date = Stage_MIN_MAX.getLong(0)
          sb.append(CT.CurrentTime() + " : stage_MIN_date :" + stage_MIN_date + " \n")
        } catch {
          case t: Throwable => {
            stage_MIN_date = 0L
          }
        }

        sb.append(CT.CurrentTime() + " : Inside updated folder processing.... \n")
        val destinationFile = UpdatedFiles_Folder_Path

        /**
         * The following code is to write file into restatement folder
         */
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(destinationFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(destinationFile))
        }
        sb.append(CT.CurrentTime() + " : Writing restatement file inside restatement folder : " + destinationFile + "\n")
        try {
          ITEM_Clean.drop("Milli_Count").repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(destinationFile)
          val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/part*.csv"))
          var path_04 = listStatusReName(0).getPath()
          hdfs.rename(path_04, new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
          sb.append(CT.CurrentTime() + " : Restatement File renaming successful. \n")
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        val stage_MAX_date = Stage_MIN_MAX.getLong(1)
        val Filtered_DF_01 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") < stage_MIN_date)
        val Filtered_DF_02 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") > stage_MAX_date)
        val Filtered_DF = Filtered_DF_01.unionAll(Filtered_DF_02)

        val Combined_Clean_DF = (Filtered_DF.unionAll(ITEM_Clean)).distinct()
        sb.append(CT.CurrentTime() + " : Started writing file inside BigFolder. \n")

        val Enrich_write_DF = Combined_Clean_DF.drop("Milli_Count")
        sb.append(CT.CurrentTime() + " : Writing file inside BigFile folder : " + Enrich_File + "\n")
        Enrich_write_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Enrich_File)
        val listStatus01 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name.replaceAll(".csv", "*.csv")))

        if (listStatus01.length > 0) {
          var path_02 = listStatus01(0).getPath()
          sb.append(CT.CurrentTime() + " : Deleting File : " + path_02.toString() + "\n")
          hdfs.delete(path_02)
        }

        //The following line will rename the enrich file
        val listStatusReName_01 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/part*.csv"))
        var path_05 = listStatusReName_01(0).getPath()
        flag = hdfs.rename(path_05, new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
        sb.append(CT.CurrentTime() + " : BigFile File renaming successful for " + Enrich_File + "\n")

        val Stage_DF_Count = Stage_File_DF.count()
        val EnrichCount = ITEM_Clean.count()
        resError_Count = Stage_DF_Count - EnrichCount

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Inventory_Calculation.IncrementalTarget - " + "Processed")
          sb.append(CT.CurrentTime() + " : Processed, MasterError file written at : " + Error_Stage_File + " \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Inventory_Calculation.IncrementalTarget - " + "Rejected")
          sb.append(CT.CurrentTime() + " : Rejected, MasterError file written at : " + Error_Stage_File + " \n")
        }

        val DataSetName = Enrich_File_Name.replaceAll(".csv", "")
        val ErrorCol = "ITEM_KEY, STORE_KEY, PERIOD_KEY"
        val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true)))

        val Harmonic_Error_DF = Clean_DF_temp.filter("ITEM_KEY == '' or STORE_KEY == '' or PERIOD_KEY == '' or PERIOD_KEY == 'Unparceable' or ITEM_KEY is null or STORE_KEY is null or PERIOD_KEY is null or ITEM_KEY == '0' or STORE_KEY == '0' or PERIOD_KEY == '0'")

        val Harmonic_ErrorDescription = "Columns: " + ErrorCol + " is not proper(null/blank/0/Unparceable)"
        val Harmonic_Crunched_rdd = Harmonic_Error_DF.rdd.map(t => Row(t.mkString(",")))
        val Harmonic_ErrorRecord = sqlContext.createDataFrame(Harmonic_Crunched_rdd, Error_Schema)
        val Harmonic_Error_Records = Harmonic_ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(Harmonic_ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val STORE_Check_Unmatched_DF = Clean_DF.except(STORE_Check)
        val STORE_Check_Crunched_rdd = STORE_Check_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val STORE_Check_ErrorRecord = sqlContext.createDataFrame(STORE_Check_Crunched_rdd, Error_Schema)
        val STORE_Check_Error_Records = STORE_Check_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: STORE_KEY")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val ITEM_Join_Clean_DF = Clean_DF.as("D3").join(Master_walmartITEM_DF.as("D4"), Clean_DF("ITEM_KEY") === Master_walmartITEM_DF("ITEM_KEY")).select($"D3.*")
        val ITEM_Clean_Unmatched_DF = Clean_DF.except(ITEM_Join_Clean_DF)
        val ITEM_Clean_Crunched_rdd = ITEM_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val ITEM_Clean_ErrorRecord = sqlContext.createDataFrame(ITEM_Clean_Crunched_rdd, Error_Schema)
        val ITEM_Clean_Error_Records = ITEM_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: ITEM_KEY")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val Harmonic_Combine_Ref_temp = Harmonic_Error_Records.unionAll(STORE_Check_Error_Records)
        val Harmonic_Combine_Ref = Harmonic_Combine_Ref_temp.unionAll(ITEM_Clean_Error_Records)

        val Date = PresentDate()
        val ErrorFolder = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File
        //val ErrorFolder = ErrorPathRecord.replaceAll("date", Date)       
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
        }
        sb.append(CT.CurrentTime() + " : Writing Harmonic & Referential Error Records inside error folder : " + ErrorFolder + "\n")

        try {
          Harmonic_Combine_Ref.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(ErrorFolder)
          val listStatusReName_002 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/part*.csv"))
          var path_002 = listStatusReName_002(0).getPath()
          val Current_Timestamp = CT.CurrentDate()
          hdfs.rename(path_002, new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/" + Enrich_File_Name.replaceAll(".csv", "") + "_" + Current_Timestamp + ".csv"))
          sb.append(CT.CurrentTime() + " : Error records File renaming successful \n")
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, Stage_File, hadoopConf, hdfs)
        res_flag = true
      } catch {
        case t: Throwable =>
          t.printStackTrace() // TODO: handle error
          sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          res_flag = false
      } finally {
        sb.append(CT.CurrentTime() + "  : IncrementWalmart method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }
      return res_flag
    }
}