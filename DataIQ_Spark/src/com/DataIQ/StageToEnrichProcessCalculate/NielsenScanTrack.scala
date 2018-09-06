package com.DataIQ.StageToEnrichProcessCalculate

import java.sql.Timestamp
import java.lang
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

class NielsenScanTrack extends java.io.Serializable {

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

  def IncrementNielsenCustomScanUPC(Stage_File: String, Enrich_File: String, Nielsen_Product: String, Nielsen_Period: String, Nielsen_DHCMarket: String, schemaString: String, Enrich_File_Name: String, Nielsen_Scan_UPC_UpdatedFilesFolder: String, FieldName: String, adl_path: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, FolderPath_temp: String, ErrorPathRecord: String): Boolean =
    {
      var res_flag = false
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: NielsenScanTrack.IncrementNielsenCustomScanUPC \n")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = 0L
      val Error_Stage_File = Stage_File.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        import sqlContext.implicits._

        // The parent path of adl (data lake store)
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf)

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Enrich_File))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Enrich_File))
        }

        // Generate the schema based on the string of schema
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        val schema_temp = schema.add(StructField("Milli_Count", LongType, nullable = true))

        sb.append(CT.CurrentTime() + " : Master File(s) loading....\n Loading Master_Nielsen_Product \n")
        val Master_Nielsen_Product = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(Nielsen_Product).select("PRDC_CODE").distinct()

        sb.append(CT.CurrentTime() + " : Master_Nielsen_Product loaded \n Loading Master_Nielsen_Period \n")
        val Master_Nielsen_Period = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(Nielsen_Period).select("DATE").distinct()

        sb.append(CT.CurrentTime() + " : Master_Nielsen_Period loaded \n Loading Master_Nielsen_DHCMkt \n")
        val Master_Nielsen_DHCMkt = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(Nielsen_DHCMarket).select("LDESC").distinct()

        sb.append(CT.CurrentTime() + " : Master_Nielsen_DHCMkt loaded \n Loading Stage_File_DF \n")
        //Load the TargetPOS_Sales Stage file into DataFtame
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File + "/*.csv")

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n")
        sb.append(CT.CurrentTime() + "  :  Doing date transform at column 'WEEKENDING' and adding column 'Milli_Count' ie milisecond count of 'WEEKENDING'......\n")
        //Apply Date Transformation
        val Transform_Date_rdd = Stage_File_DF.rdd.map(t => Row(t.get(0), t.get(1), day(t.getString(2), "yyyy-MM-dd"), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27), t.get(28), t.get(29), t.get(30), t.get(31), t.get(32), t.get(33), t.get(34), t.get(35), t.get(36), t.get(37), t.get(38), t.get(39), t.get(40), t.get(41), t.get(42), t.get(43), t.get(44), t.get(45), t.get(46), t.get(47), t.get(48), t.get(49), t.get(50), t.get(51), t.get(52), t.get(53), t.get(54), t.get(55), day(t.getString(56), "yyyy-MM-dd"), To_Milli(t.getString(2), "yyyy-MM-dd")))
        val Clean_DF_temp01 = sqlContext.createDataFrame(Transform_Date_rdd, schema_temp)
        sb.append(CT.CurrentTime() + " : Doing left padding to 13 character on UPC field.... \n")
        val Clean_DF_temp = Clean_DF_temp01.withColumn("UPC", lpad(Clean_DF_temp01("UPC"), 13, "0"))

        sb.append(CT.CurrentTime() + "  :  Doing filter on data for harmonization.......\n")
        //Clean DataFrame
        val Clean_DF = Clean_DF_temp.filter("GEO != '' and UPC != '' and WEEKENDING != '' and WEEKENDING != 'Unparceable' and GEO is not null and UPC is not null and WEEKENDING is not null and GEO != '0' and UPC != '0' and WEEKENDING != '0'")

        var Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        try {
          sb.append(CT.CurrentTime() + "  :  Reading enriched file from folder : " + Enrich_File + "\n")
          val temp_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(Enrich_File + "/*.csv")
          Enrich_DF = Enrich_DF.union(temp_Enrich_DF)
        } catch {
          case t: Throwable =>
            {
              sb.append(CT.CurrentTime() + " : No file present in enriched folder, seems like first time processing of this POS data..... \n")
            }
        }
        sb.append(CT.CurrentTime() + " : Enrich dataframe reading completed. \n")

        sb.append(CT.CurrentTime() + " : Referential integrity validation started.... \n")
        /**
         * Following two lines are for refrential integrity
         */
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'UPC'...... \n")
        val UPC_Check = Clean_DF.as("D1").join(Master_Nielsen_Product.as("D2"), Clean_DF("UPC") === Master_Nielsen_Product("PRDC_CODE")).select($"D1.*")
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'WEEKENDING'...... \n")
        val WEEKENDING_Clean = UPC_Check.as("D3").join(Master_Nielsen_Period.as("D4"), UPC_Check("WEEKENDING") === Master_Nielsen_Period("DATE")).select($"D3.*")
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'GEO'...... \n")
        val GEO_Clean = WEEKENDING_Clean.as("D5").join(Master_Nielsen_DHCMkt.as("D6"), upper(WEEKENDING_Clean.col("GEO")) === upper(Master_Nielsen_DHCMkt.col("LDESC"))).select($"D6.LDESC", $"D5.*").drop($"D5.GEO").withColumnRenamed("LDESC", "GEO")

        sb.append(CT.CurrentTime() + " : Referential integrity validation completed.... \n")
        /**
         * Following code is for date compare, ie to check if the file is incremental or restatement.
         */
        var dateCom_flag = ""

        sb.append(CT.CurrentTime() + "  : Date compare calculation started for detection weather Restatement or Incremental......\n")
        val Temp_Enrich_rdd = Enrich_DF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27), t.get(28), t.get(29), t.get(30), t.get(31), t.get(32), t.get(33), t.get(34), t.get(35), t.get(36), t.get(37), t.get(38), t.get(39), t.get(40), t.get(41), t.get(42), t.get(43), t.get(44), t.get(45), t.get(46), t.get(47), t.get(48), t.get(49), t.get(50), t.get(51), t.get(52), t.get(53), t.get(54), t.get(55), t.get(56), To_Milli(t.getString(2), "MM/dd/yyyy")))
        val Temp_Enrich_DF = sqlContext.createDataFrame(Temp_Enrich_rdd, schema_temp)

        var stage_MIN_date = 0L
        val Stage_MIN_MAX = GEO_Clean.select("Milli_Count").agg(min(col("Milli_Count")), max(col("Milli_Count"))).collect().apply(0)
        try {
          stage_MIN_date = Stage_MIN_MAX.getLong(0)
          sb.append(CT.CurrentTime() + " : stage_MIN_date :" + stage_MIN_date + " \n")
        } catch {
          case t: Throwable => {
            stage_MIN_date = 0L
          }
        }

        sb.append(CT.CurrentTime() + " : Inside Updated Folder processing.... \n")
        val destinationFile = Nielsen_Scan_UPC_UpdatedFilesFolder

        /**
         * The following code is to write file into restatement folder
         */
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(destinationFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(destinationFile))
        }
        try {
          sb.append(CT.CurrentTime() + " : Writing Updated file inside Updated folder : " + destinationFile + "\n")
          GEO_Clean.drop("Milli_Count").repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(destinationFile)
          val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/part*.csv"))
          var path_04 = listStatusReName(0).getPath()
          hdfs.rename(path_04, new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
          sb.append(CT.CurrentTime() + " : Updated File renaming successful. \n")
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        val stage_MAX_date = Stage_MIN_MAX.getLong(1)
        //val Filtered_DF = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") < stage_MIN_date && Temp_Enrich_DF("Milli_Count") > stage_MAX_date)
        val Filtered_DF_01 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") < stage_MIN_date)
        val Filtered_DF_02 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") > stage_MAX_date)
        val Filtered_DF = Filtered_DF_01.unionAll(Filtered_DF_02)
        val Combined_Clean_DF = (Filtered_DF.unionAll(GEO_Clean)).distinct()

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
        val EnrichCount = GEO_Clean.count()
        resError_Count = Stage_DF_Count - EnrichCount

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
          //sb.append(CT.CurrentTime() + " : Processed, MasterError file written at : " + Error_Stage_File + " \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
          //sb.append(CT.CurrentTime() + " : Rejected, MasterError file written at : " + Error_Stage_File + " \n")
        }

        val DataSetName = Enrich_File_Name.replaceAll(".csv", "")
        val ErrorCol = "GEO, UPC, WEEKENDING"
        val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true)))

        sb.append(CT.CurrentTime() + "  : Harmonic_Error_DF calculation has started........\n")
        val Harmonic_Error_DF = Clean_DF_temp.filter("GEO == '' or UPC == '' or WEEKENDING == '' or WEEKENDING == 'Unparceable' or GEO is null or UPC is null or WEEKENDING is null or GEO == '0' or UPC == '0' or WEEKENDING == '0'")

        val Harmonic_ErrorDescription = "Columns: " + ErrorCol + " is not proper(null/blank/0/Unparceable)"
        val Harmonic_Crunched_rdd = Harmonic_Error_DF.rdd.map(t => Row(t.mkString(",")))
        val Harmonic_ErrorRecord = sqlContext.createDataFrame(Harmonic_Crunched_rdd, Error_Schema)
        val Harmonic_Error_Records = Harmonic_ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(Harmonic_ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        sb.append(CT.CurrentTime() + "  : Referential error calculation has started........\n")
        val UPC_Check_Unmatched_DF = Clean_DF.except(UPC_Check)
        val UPC_Check_Crunched_rdd = UPC_Check_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val UPC_Check_ErrorRecord = sqlContext.createDataFrame(UPC_Check_Crunched_rdd, Error_Schema)
        val UPC_Check_Error_Records = UPC_Check_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: UPC")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val Weekending_Join_Clean_DF = Clean_DF.as("D3").join(Master_Nielsen_Period.as("D4"), Clean_DF("WEEKENDING") === Master_Nielsen_Period("DATE")).select($"D3.*")
        val WEEKENDING_Clean_Unmatched_DF = Clean_DF.except(Weekending_Join_Clean_DF)
        val WEEKENDING_Clean_Crunched_rdd = WEEKENDING_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val WEEKENDING_Clean_ErrorRecord = sqlContext.createDataFrame(WEEKENDING_Clean_Crunched_rdd, Error_Schema)
        val WEEKENDING_Clean_Error_Records = WEEKENDING_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: WEEKENDING")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val GEO_Join_Clean_DF = Clean_DF.as("D5").join(Master_Nielsen_DHCMkt.as("D6"), upper(Clean_DF("GEO")) === upper(Master_Nielsen_DHCMkt("LDESC"))).select($"D5.*")
        val GEO_Clean_Unmatched_DF = Clean_DF.except(GEO_Join_Clean_DF)
        val GEO_Clean_Crunched_rdd = GEO_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
        val GEO_Clean_ErrorRecord = sqlContext.createDataFrame(GEO_Clean_Crunched_rdd, Error_Schema)
        val GEO_Clean_Error_Records = GEO_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: GEO")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")

        val Harmonic_Combine_Ref_temp = Harmonic_Error_Records.unionAll(UPC_Check_Error_Records)
        val Harmonic_Combine_Ref_temp01 = Harmonic_Combine_Ref_temp.unionAll(WEEKENDING_Clean_Error_Records)
        val Harmonic_Combine_Ref = Harmonic_Combine_Ref_temp01.unionAll(GEO_Clean_Error_Records)

        val Date = PresentDate()
        val ErrorFolder = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
        }
        try {
          sb.append(CT.CurrentTime() + " : Writing Harmonic & Referential Error Records inside error folder : " + ErrorFolder + "\n")
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
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }
      return res_flag
    }
}