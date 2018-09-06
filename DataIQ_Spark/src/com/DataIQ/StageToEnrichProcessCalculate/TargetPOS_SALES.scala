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

class TargetPOS_SALES extends java.io.Serializable {
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

  def IncrementalTarget(Stage_File: String, Enrich_File: String, TargetPOS_Product: String, TargetPOS_Location: String, schemaString: String, Enrich_File_Name: String, UpdatedFiles_Folder_Path: String, FieldName: String, adl_path: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, FolderPath_temp: String, ErrorPathRecord: String): Boolean =
    {
      var res_flag = false
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: TargetPOS_SALES.IncrementalTarget \n")

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

        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Product......\n")
        //Load the TargetPOS_Product file into DataFrame, select only one column :- "ProductNbr" as we require only this column
        val Master_ProductNbr_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(TargetPOS_Product).select("ProductNbr").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Product is complete.\n")
        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Location.......\n")
        //Load the TargetPOS_Location file into DataFrame, select only one column :- "LocationNbr" as we require only this column 
        val Master_LocationNbr_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(TargetPOS_Location).select("LocationNbr").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Location is complete.\n")

        //Load the TargetPOS_Sales Stage file into DataFtame
        sb.append(CT.CurrentTime() + "  :  Reading all stage files at location : " + Stage_File + ".......\n")
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File + "/*.csv")
        sb.append(CT.CurrentTime() + "  :  Reading stage file is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Doing date transform at column 'TransWeek' and adding column 'Milli_Count' ie milisecond count of 'TransWeek'......\n")
        //Apply Date Transformation
        val Transform_Date_rdd = Stage_File_DF.rdd.map(t => Row(day(t.getString(0), "yyyy-MM-dd"), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), To_Milli(t.getString(0), "yyyy-MM-dd")))
        val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, schema_temp)

        sb.append(CT.CurrentTime() + "  :  Doing filter on data for harmonization.......\n")
        //Clean DataFrame
        val Clean_DF = Clean_DF_temp.filter("LocationNbr != '' and ProductNbr != '' and TransWeek != '' and TransWeek != 'Unparceable' and LocationNbr is not null and ProductNbr is not null and TransWeek is not null and LocationNbr != '0' and ProductNbr != '0' and TransWeek != '0'")

        var Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        try {
          sb.append(CT.CurrentTime() + "  :  Reading enriched file from folder : " + Enrich_File + "\n")
          val temp_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(Enrich_File + "/*.csv")
          Enrich_DF = Enrich_DF.union(temp_Enrich_DF)
        } catch {
          case t: Throwable =>
            {
              sb.append(CT.CurrentTime() + "  :  Exception occurred while reading enriched file from folder : " + Enrich_File + ", maybe the file is not present at the given location (first time processing)\n")
            }
        }

        /**
         * Following two lines are for refrential integrity
         */

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'ProductNbr'...... \n")
        val ProductNbr_Check = Clean_DF.as("D1").join(Master_ProductNbr_DF.as("D2"), Clean_DF("ProductNbr") === Master_ProductNbr_DF("ProductNbr")).select($"D1.*")
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'LocationNbr'...... \n")
        val LocationNbr_Clean = ProductNbr_Check.as("D3").join(Master_LocationNbr_DF.as("D4"), ProductNbr_Check("LocationNbr") === Master_LocationNbr_DF("LocationNbr")).select($"D3.*")

        /**
         * Following code is for date compare, ie to check if the file is incremental or restatement.
         */
        sb.append(CT.CurrentTime() + "  : Date compare calculation started for detection weather Restatement or Incremental......\n")
        var dateCom_flag = ""

        val Temp_Enrich_rdd = Enrich_DF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), To_Milli(t.getString(0), "MM/dd/yyyy")))
        val Temp_Enrich_DF = sqlContext.createDataFrame(Temp_Enrich_rdd, schema_temp)

        var stage_MIN_date = 0L
        val Stage_MIN_MAX = LocationNbr_Clean.select("Milli_Count").agg(min(col("Milli_Count")), max(col("Milli_Count"))).collect().apply(0)
        try {
          stage_MIN_date = Stage_MIN_MAX.getLong(0)
          sb.append(CT.CurrentTime() + " : stage_MIN_date :" + stage_MIN_date + " \n")
        } catch {
          case t: Throwable => {
            stage_MIN_date = 0L
          }
        }

        sb.append(CT.CurrentTime() + "  : Inside the updated block.\n")
        //val destinationFile = Restatement_Folder_Path
        val destinationFile = UpdatedFiles_Folder_Path

        /**
         * The following code is to write file into restatement folder
         */
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(destinationFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(destinationFile))
        }
        try {
          sb.append(CT.CurrentTime() + "  : Writing the restatement file in folder : " + destinationFile + "......\n")
          LocationNbr_Clean.drop("Milli_Count").repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(destinationFile)
          val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/part*.csv"))
          var path_04 = listStatusReName(0).getPath()
          sb.append(CT.CurrentTime() + "  : Renameing the restatement file......\n")
          hdfs.rename(path_04, new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        val stage_MAX_date = Stage_MIN_MAX.getLong(1)
        val Filtered_DF_01 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") < stage_MIN_date)
        val Filtered_DF_02 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") > stage_MAX_date)
        val Filtered_DF = Filtered_DF_01.unionAll(Filtered_DF_02)

        val Combined_Clean_DF = (Filtered_DF.unionAll(LocationNbr_Clean)).distinct()

        val Enrich_write_DF = Combined_Clean_DF.drop("Milli_Count")
        sb.append(CT.CurrentTime() + "  : Writing the Big file in big file folder : " + Enrich_File + "......\n")
        Enrich_write_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Enrich_File)
        val listStatus01 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name.replaceAll(".csv", "*.csv")))

        if (listStatus01.length > 0) {
          var path_02 = listStatus01(0).getPath()
          hdfs.delete(path_02)
        }

        //The following line will rename the enrich file
        val listStatusReName_01 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/part*.csv"))
        var path_05 = listStatusReName_01(0).getPath()
        sb.append(CT.CurrentTime() + "  : Renameing the Big file in big file folder......\n")
        flag = hdfs.rename(path_05, new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))

        sb.append(CT.CurrentTime() + "  : Error calculation and writing part started........\n")

        val Stage_DF_Count = Stage_File_DF.count()
        val EnrichCount = LocationNbr_Clean.count()
        resError_Count = Stage_DF_Count - EnrichCount

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Sales_Calculation.IncrementalTarget - " + "Processed")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Sales_Calculation.IncrementalTarget - " + "Rejected")
        }

        val DataSetName = Enrich_File_Name.replaceAll(".csv", "")
        val ErrorCol = "LocationNbr, ProductNbr, TransWeek"
        val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true)))

        sb.append(CT.CurrentTime() + "  : Harmonic_Error_DF calculation has started........\n")
        val Harmonic_Error_DF = Clean_DF_temp.filter("LocationNbr == '' or ProductNbr == '' or TransWeek == '' or TransWeek == 'Unparceable' or LocationNbr is null or ProductNbr is null or TransWeek is null or LocationNbr == '0' or ProductNbr == '0' or TransWeek == '0'")

        val Harmonic_ErrorDescription = "Columns: " + ErrorCol + " is not proper(null/blank/0/Unparceable)"
        val Harmonic_Crunched_rdd = Harmonic_Error_DF.rdd.map(t => Row(t.mkString(",")))
        val Harmonic_ErrorRecord = sqlContext.createDataFrame(Harmonic_Crunched_rdd, Error_Schema)
        val Harmonic_Error_Records = Harmonic_ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(Harmonic_ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        sb.append(CT.CurrentTime() + "  : Referential error calculation has started........\n")
        val ProductNbr_Check_Unmatched_DF = Clean_DF.except(ProductNbr_Check)
        val ProductNbr_Check_Crunched_rdd = ProductNbr_Check_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val ProductNbr_Check_ErrorRecord = sqlContext.createDataFrame(ProductNbr_Check_Crunched_rdd, Error_Schema)
        val ProductNbr_Check_Error_Records = ProductNbr_Check_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: ProductNbr")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val LocationNbr_Join_Clean_DF = Clean_DF.as("D3").join(Master_LocationNbr_DF.as("D4"), Clean_DF("LocationNbr") === Master_LocationNbr_DF("LocationNbr")).select($"D3.*")
        val LocationNbr_Clean_Unmatched_DF = Clean_DF.except(LocationNbr_Join_Clean_DF)
        val LocationNbr_Clean_Crunched_rdd = LocationNbr_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val LocationNbr_Clean_ErrorRecord = sqlContext.createDataFrame(LocationNbr_Clean_Crunched_rdd, Error_Schema)
        val LocationNbr_Clean_Error_Records = LocationNbr_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: LocationNbr")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val Harmonic_Combine_Ref_temp = Harmonic_Error_Records.unionAll(ProductNbr_Check_Error_Records)
        val Harmonic_Combine_Ref = Harmonic_Combine_Ref_temp.unionAll(LocationNbr_Clean_Error_Records)

        val Date = PresentDate()
        val ErrorFolder = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File
        //val ErrorFolder = ErrorPathRecord.replaceAll("date", Date)       
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
        }
        try {
          sb.append(CT.CurrentTime() + "  : Writing error record file at location : " + ErrorFolder + "........\n")
          Harmonic_Combine_Ref.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(ErrorFolder)
          val listStatusReName_002 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/part*.csv"))
          var path_002 = listStatusReName_002(0).getPath()
          val Current_Timestamp = CT.CurrentDate()
          sb.append(CT.CurrentTime() + "  : Renameing error record file at location : " + ErrorFolder + "........\n")
          hdfs.rename(path_002, new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/" + Enrich_File_Name.replaceAll(".csv", "") + "_" + Current_Timestamp + ".csv"))

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
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }
      return res_flag
    }
}