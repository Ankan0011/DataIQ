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

class TargetPOS_GRM extends java.io.Serializable {
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

  /**
   * New Target GRM CR start here
   *
   *
   *
   */
  def IncrementalTargetGRM(Stage_File: String, Enrich_File: String, STR_CNCPT: String, GST_DEMOS: String, STR_CURR: String, ITEM_HIER: String, schemaString: String, Enrich_File_Name: String, UpdatedFiles_Folder_Path: String, FieldName: String, adl_path: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, FolderPath_temp: String, ErrorPathRecord: String): Boolean =
    {
      var res_flag = false
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: TargetPOS_GRM.IncrementalTarget.........\n")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = 0L
      val Error_Stage_File = Stage_File.replaceAll("/Stage", "").replaceAll(adl_path, "")
      val Temp_Folder = Enrich_File.replaceAll("BigFile", "Temp")
      
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

        sb.append(CT.CurrentTime() + "  :  Reading master file : STR_CNCPT......\n")
        val STR_CNCPT_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(STR_CNCPT)
        val STR_CNCPT_CO_LOC_I_DF = STR_CNCPT_DF.select("CO_LOC_I").distinct()
        val STR_CNCPT_CO_LOC_REF_I_DF = STR_CNCPT_DF.select("CO_LOC_REF_I").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : STR_CNCPT is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Reading master file : GST_DEMOS.......\n")
        val GST_DEMOS_GST_REF_I_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(GST_DEMOS).select("GST_REF_I").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : GST_DEMOS is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Reading master file : STR_CURR.......\n")
        val STR_CURR_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(STR_CURR)
        val STR_CURR_SITE_SEQ_I_DF = STR_CURR_DF.select("SITE_SEQ_I").distinct()
        val STR_CURR_STR_I_DF = STR_CURR_DF.select("STR_I").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : STR_CURR is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Reading master file : ITEM_HIER.......\n")
        val ITEM_HIER_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(ITEM_HIER)
        val ITEM_HIER_MDSE_ITEM_I_DF = ITEM_HIER_DF.select("MDSE_ITEM_I").distinct()
        val ITEM_HIER_MDSE_ITEM_REF_I_DF = ITEM_HIER_DF.select("MDSE_ITEM_REF_I").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : ITEM_HIER is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Reading all stage files at location : " + Stage_File + ".......\n")
        //Load the TargetPOS_Sales Stage file into DataFtame

        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File + "/*/*")
       
        sb.append(CT.CurrentTime() + "  :  Reading stage file is complete.\n")

        //Apply Date Transformation
        sb.append(CT.CurrentTime() + "  :  Doing date transform at column 'TransDate' and adding column 'Milli_Count' ie milisecond count of 'TransDate'......\n")
        val Transform_Date_rdd = Stage_File_DF.rdd.map(t => Row(t.get(0), t.get(1), day(t.getString(2), "yyyy-MM-dd"), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18)))
        val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, schema)

        //Clean DataFrame
        sb.append(CT.CurrentTime() + "  :  Doing filter on data for harmonization.......\n")
        val Clean_DF = Clean_DF_temp.filter("GST_REF_I != '' and MDSE_ITEM_I != '' and MDSE_ITEM_REF_I != '' and SLS_D != '' and CO_LOC_I != '' and CO_LOC_REF_I != '' and SLS_D != 'Unparceable' and GST_REF_I is not null and MDSE_ITEM_I is not null and MDSE_ITEM_REF_I is not null and SLS_D is not null and CO_LOC_I is not null and CO_LOC_REF_I is not null and GST_REF_I != '0' and MDSE_ITEM_I != '0' and MDSE_ITEM_REF_I != '0' and SLS_D != '0' and CO_LOC_I != '0' and CO_LOC_REF_I != '0'")

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
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'Clean_DF.GST_REF_I' with 'GST_DEMOS_GST_REF_I_DF.GST_REF_I'...... \n")
        val GST_DEMOS_Clean = Clean_DF.as("D1").join(GST_DEMOS_GST_REF_I_DF.as("D2"), Clean_DF("GST_REF_I") === GST_DEMOS_GST_REF_I_DF("GST_REF_I")).select($"D1.*")

       sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'GST_DEMOS_Clean.CO_LOC_REF_I' with 'STR_CNCPT_CO_LOC_REF_I_DF.CO_LOC_REF_I'....... \n")
        val STR_CNCPT_CO_LOC_REF_I_Clean = GST_DEMOS_Clean.as("D3").join(STR_CNCPT_CO_LOC_REF_I_DF.as("D4"), GST_DEMOS_Clean("CO_LOC_REF_I") === STR_CNCPT_CO_LOC_REF_I_DF("CO_LOC_REF_I")).select($"D3.*")

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'STR_CNCPT_CO_LOC_REF_I_Clean.CO_LOC_REF_I' with 'STR_CURR_STR_I_DF.STR_I'....... \n")
        val STR_CURR_STR_I_Clean = STR_CNCPT_CO_LOC_REF_I_Clean.as("D5").join(STR_CURR_STR_I_DF.as("D6"), STR_CNCPT_CO_LOC_REF_I_Clean("CO_LOC_REF_I") === STR_CURR_STR_I_DF("STR_I")).select($"D5.*")

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'STR_CURR_STR_I_Clean.CO_LOC_I' with 'STR_CNCPT_CO_LOC_I_DF.CO_LOC_I'....... \n")
        val STR_CNCPT_CO_LOC_I_Clean = STR_CURR_STR_I_Clean.as("D7").join(STR_CNCPT_CO_LOC_I_DF.as("D8"), STR_CURR_STR_I_Clean("CO_LOC_I") === STR_CNCPT_CO_LOC_I_DF("CO_LOC_I")).select($"D7.*")

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'STR_CNCPT_CO_LOC_I_Clean.CO_LOC_I' with 'STR_CURR_SITE_SEQ_I_DF.SITE_SEQ_I'....... \n")
        val STR_CURR_SITE_SEQ_I_Clean = STR_CNCPT_CO_LOC_I_Clean.as("D9").join(STR_CURR_SITE_SEQ_I_DF.as("D10"), STR_CNCPT_CO_LOC_I_Clean("CO_LOC_I") === STR_CURR_SITE_SEQ_I_DF("SITE_SEQ_I")).select($"D9.*")

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'STR_CURR_SITE_SEQ_I_Clean.MDSE_ITEM_I' with 'ITEM_HIER_MDSE_ITEM_I_DF.MDSE_ITEM_I'....... \n")
        val ITEM_HIER_MDSE_ITEM_I_Clean = STR_CURR_SITE_SEQ_I_Clean.as("D11").join(ITEM_HIER_MDSE_ITEM_I_DF.as("D12"), STR_CURR_SITE_SEQ_I_Clean("MDSE_ITEM_I") === ITEM_HIER_MDSE_ITEM_I_DF("MDSE_ITEM_I")).select($"D11.*")

        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'ITEM_HIER_MDSE_ITEM_I_Clean.MDSE_ITEM_REF_I' with 'ITEM_HIER_MDSE_ITEM_REF_I_DF.MDSE_ITEM_REF_I'....... \n")
        val ITEM_HIER_MDSE_ITEM_REF_I_Clean = ITEM_HIER_MDSE_ITEM_I_Clean.as("D13").join(ITEM_HIER_MDSE_ITEM_REF_I_DF.as("D14"), ITEM_HIER_MDSE_ITEM_I_Clean("MDSE_ITEM_REF_I") === ITEM_HIER_MDSE_ITEM_REF_I_DF("MDSE_ITEM_REF_I")).select($"D13.*")

        

        sb.append(CT.CurrentTime() + "  : Inside the Updated block.\n")
        val destinationFile = UpdatedFiles_Folder_Path

        /**
         * The following code is to write file into incremental folder
         */
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(destinationFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(destinationFile))
        }

        if (hdfs.isDirectory(new org.apache.hadoop.fs.Path(Temp_Folder))) {
          hdfs.delete(new org.apache.hadoop.fs.Path(Temp_Folder))
        }
        hdfs.mkdirs(new org.apache.hadoop.fs.Path(Temp_Folder+"/Updated"))
        hdfs.mkdirs(new org.apache.hadoop.fs.Path(Temp_Folder+"/BigTemp"))
        try {
          sb.append(CT.CurrentTime() + "  : Writing the file in folder : " + destinationFile + "......\n")
          ITEM_HIER_MDSE_ITEM_REF_I_Clean.write.option("header", "true").option("escape", "\"").mode("append").csv(Temp_Folder+"/Updated")
          val UpdatedDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(Temp_Folder+"/Updated/*")
          
          UpdatedDF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(destinationFile)
          
          val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/part*.csv"))
          var path_04 = listStatusReName(0).getPath()
          sb.append(CT.CurrentTime() + "  : Renameing the restatement file......\n")
          hdfs.rename(path_04, new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/" + Enrich_File_Name))

        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        val Update_DatedDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(destinationFile)
        /**
         * Following code is for date compare, ie to check if the file is incremental or restatement.
         */
        sb.append(CT.CurrentTime() + "  : Date compare calculation started for detection weather Restatement or Incremental......\n")
        var dateCom_flag = ""

        val Temp_Enrich_rdd = Enrich_DF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), To_Milli(t.getString(2), "MM/dd/yyyy")))
        val Temp_Enrich_DF = sqlContext.createDataFrame(Temp_Enrich_rdd, schema_temp)

        var stage_MIN_date = 0L
        var stage_MAX_date = 0L
        val MilliCount_rdd = Update_DatedDF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), To_Milli(t.getString(2), "MM/dd/yyyy")))
        val MilliCount_DF = sqlContext.createDataFrame(MilliCount_rdd, schema_temp)
        
       
        
        val Stage_MIN_MAX = MilliCount_DF.select("Milli_Count").agg(min(col("Milli_Count")), max(col("Milli_Count"))).collect().apply(0)
        try {
          stage_MIN_date = Stage_MIN_MAX.getLong(0)
          stage_MAX_date = Stage_MIN_MAX.getLong(1)
          sb.append(CT.CurrentTime() + " : stage_MIN_date :" + stage_MIN_date + " \n")
          sb.append(CT.CurrentTime() + " : stage_MAX_date :" + stage_MAX_date + " \n")
        } catch {
          case t: Throwable => {
            stage_MIN_date = 0L
          }
        }
        
        
        val Filtered_DF_01 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") < stage_MIN_date)
        val Filtered_DF_02 = Temp_Enrich_DF.where(Temp_Enrich_DF("Milli_Count") > stage_MAX_date)
        val Filtered_DF = Filtered_DF_01.unionAll(Filtered_DF_02)
       
        val Combined_Clean_DF = (Filtered_DF.unionAll(MilliCount_DF)).distinct()

        val Enrich_write_DF = Combined_Clean_DF.drop("Milli_Count")

        sb.append(CT.CurrentTime() + "  : Writing the Big file in big file folder : " + Enrich_File + "......\n")
        Enrich_write_DF.write.option("header", "true").option("escape", "\"").mode("append").csv(Temp_Folder+"/BigTemp")
        val BigDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(Temp_Folder+"/BigTemp/*")
        
        BigDF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Enrich_File)
        
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
        val EnrichCount = ITEM_HIER_MDSE_ITEM_REF_I_Clean.count()
        resError_Count = Stage_DF_Count - EnrichCount

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count.toString(), EnrichCount.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
        }

        
        val DataSetName = Enrich_File_Name.replaceAll(".csv", "")
        val ErrorCol = "GST_REF_I, MDSE_ITEM_I, MDSE_ITEM_REF_I,SLS_D,CO_LOC_I,CO_LOC_REF_I"
        val Error_Schema = StructType(Array(StructField("Error_Record", StringType, true)))

        val Date = PresentDate()

        val listStatus_StageFile = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Stage_File + "/*".replaceAll(adl_path, "")))

        if (!listStatus_StageFile.isEmpty) {

          for (i <- 0 to (listStatus_StageFile.length - 1)) {
            val Stage_File_Ind = listStatus_StageFile.apply(i).getPath.toString()
            val FolderName = Stage_File_Ind.split("/").last

            val Stage_File_Error_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File_Ind + "/*")
            val Transform_Error_Date_rdd = Stage_File_Error_DF.rdd.map(t => Row(t.get(0), t.get(1), day(t.getString(2), "yyyy-MM-dd"), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18)))
            val Clean_DF_Error_temp = sqlContext.createDataFrame(Transform_Error_Date_rdd, schema)

            //Clean DataFrame
            sb.append(CT.CurrentTime() + "  :  Doing filter on data for harmonization.......\n")
            val Clean_Error_DF = Clean_DF_Error_temp.filter("GST_REF_I != '' and MDSE_ITEM_I != '' and MDSE_ITEM_REF_I != '' and SLS_D != '' and CO_LOC_I != '' and CO_LOC_REF_I != '' and SLS_D != 'Unparceable' and GST_REF_I is not null and MDSE_ITEM_I is not null and MDSE_ITEM_REF_I is not null and SLS_D is not null and CO_LOC_I is not null and CO_LOC_REF_I is not null and GST_REF_I != '0' and MDSE_ITEM_I != '0' and MDSE_ITEM_REF_I != '0' and SLS_D != '0' and CO_LOC_I != '0' and CO_LOC_REF_I != '0'")

            val ErrorFolder = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File + "/AllError/" + FolderName
            if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
              hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
            }
            val TempFolder = ErrorFolder
            sb.append(CT.CurrentTime() + "  : Harmonic_Error_DF calculation has started........\n")
            val Harmonic_Error_DF = Clean_DF_Error_temp.filter("GST_REF_I == '' or MDSE_ITEM_I == '' or MDSE_ITEM_REF_I == '' or SLS_D == '' or CO_LOC_I == '' or CO_LOC_REF_I == '' or SLS_D == 'Unparceable' or GST_REF_I is null or MDSE_ITEM_I is null or MDSE_ITEM_REF_I is null or SLS_D is null or CO_LOC_I is null or CO_LOC_REF_I is null or GST_REF_I == '0' or MDSE_ITEM_I == '0' or MDSE_ITEM_REF_I == '0' or SLS_D == '0' or CO_LOC_I == '0' or CO_LOC_REF_I == '0'")

            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/Harmonic"))
            val Harmonic_ErrorDescription = "Columns: " + ErrorCol + " is not proper(null/blank/0/Unparceable)"
            val Harmonic_Crunched_rdd = Harmonic_Error_DF.rdd.map(t => Row(t.mkString(",")))
            val Harmonic_ErrorRecord = sqlContext.createDataFrame(Harmonic_Crunched_rdd, Error_Schema)
            val Harmonic_Error_Records = Harmonic_ErrorRecord.withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Error_Description", lit(Harmonic_ErrorDescription)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            Harmonic_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/Harmonic")

             hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/ITEM_HIER_MDSE"))
            val ITEM_HIER_MDSE_ITEM_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD11").join(ITEM_HIER_MDSE_ITEM_I_DF.as("DD12"), Clean_Error_DF("MDSE_ITEM_I") === ITEM_HIER_MDSE_ITEM_I_DF("MDSE_ITEM_I")).select($"DD11.*")
            val ITEM_HIER_MDSE_ITEM_I_Clean_Unmatched_DF = Clean_Error_DF.except(ITEM_HIER_MDSE_ITEM_I_DF_Join_Clean_DF)
            val ITEM_HIER_MDSE_ITEM_I_Clean_Crunched_rdd = ITEM_HIER_MDSE_ITEM_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val ITEM_HIER_MDSE_ITEM_I_Clean_ErrorRecord = sqlContext.createDataFrame(ITEM_HIER_MDSE_ITEM_I_Clean_Crunched_rdd, Error_Schema)
            val ITEM_HIER_MDSE_ITEM_I_Clean_Error_Records = ITEM_HIER_MDSE_ITEM_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: MDSE_ITEM_I when mapped to ITEM_HIER.MDSE_ITEM_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            ITEM_HIER_MDSE_ITEM_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/ITEM_HIER_MDSE")


            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/GST_DEMO"))
            val GST_DEMOS_Clean_Unmatched_DF = Clean_Error_DF.except(GST_DEMOS_Clean)
            val GST_DEMOS_Clean_Crunched_rdd = GST_DEMOS_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val GST_DEMOS_Clean_ErrorRecord = sqlContext.createDataFrame(GST_DEMOS_Clean_Crunched_rdd, Error_Schema)
            val GST_DEMOS_Clean_Error_Records = GST_DEMOS_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: GST_REF_I when mapped to GST_DEMOS.GST_REF_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            GST_DEMOS_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/GST_DEMO")

            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/STR_CNCPT"))
            val STR_CNCPT_CO_LOC_REF_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD3").join(STR_CNCPT_CO_LOC_REF_I_DF.as("DD4"), Clean_Error_DF("CO_LOC_REF_I") === STR_CNCPT_CO_LOC_REF_I_DF("CO_LOC_REF_I")).select($"DD3.*")
            val STR_CNCPT_CO_LOC_REF_I_Clean_Unmatched_DF = Clean_Error_DF.except(STR_CNCPT_CO_LOC_REF_I_DF_Join_Clean_DF)
            val STR_CNCPT_CO_LOC_REF_I_Clean_Crunched_rdd = STR_CNCPT_CO_LOC_REF_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val STR_CNCPT_CO_LOC_REF_I_Clean_ErrorRecord = sqlContext.createDataFrame(STR_CNCPT_CO_LOC_REF_I_Clean_Crunched_rdd, Error_Schema)
            val STR_CNCPT_CO_LOC_REF_I_Clean_Error_Records = STR_CNCPT_CO_LOC_REF_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: CO_LOC_REF_I when mapped to STR_CNCPT.CO_LOC_REF_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            STR_CNCPT_CO_LOC_REF_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/STR_CNCPT")


            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/STR_CURR"))
            val STR_CURR_STR_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD5").join(STR_CURR_STR_I_DF.as("DD6"), Clean_Error_DF("CO_LOC_REF_I") === STR_CURR_STR_I_DF("STR_I")).select($"DD5.*")
            val STR_CURR_STR_I_Clean_Unmatched_DF = Clean_Error_DF.except(STR_CURR_STR_I_DF_Join_Clean_DF)
            val STR_CURR_STR_I_Clean_Crunched_rdd = STR_CURR_STR_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val STR_CURR_STR_I_Clean_ErrorRecord = sqlContext.createDataFrame(STR_CURR_STR_I_Clean_Crunched_rdd, Error_Schema)
            val STR_CURR_STR_I_Clean_Error_Records = STR_CURR_STR_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: CO_LOC_REF_I when mapped to STR_CURR.STR_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            STR_CURR_STR_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/STR_CURR")


            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/STR_CNCPT_CO"))
            val STR_CNCPT_CO_LOC_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD7").join(STR_CNCPT_CO_LOC_I_DF.as("DD8"), Clean_Error_DF("CO_LOC_I") === STR_CNCPT_CO_LOC_I_DF("CO_LOC_I")).select($"DD7.*")
            val STR_CNCPT_CO_LOC_I_Clean_Unmatched_DF = Clean_Error_DF.except(STR_CNCPT_CO_LOC_I_DF_Join_Clean_DF)
            val STR_CNCPT_CO_LOC_I_Clean_Crunched_rdd = STR_CNCPT_CO_LOC_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val STR_CNCPT_CO_LOC_I_Clean_ErrorRecord = sqlContext.createDataFrame(STR_CNCPT_CO_LOC_I_Clean_Crunched_rdd, Error_Schema)
            val STR_CNCPT_CO_LOC_I_Clean_Error_Records = STR_CNCPT_CO_LOC_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: CO_LOC_I when mapped to STR_CNCPT.CO_LOC_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            STR_CNCPT_CO_LOC_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/STR_CNCPT_CO")


            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/STR_CURR_SITE"))
            val STR_CURR_SITE_SEQ_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD9").join(STR_CURR_SITE_SEQ_I_DF.as("DD10"), Clean_Error_DF("CO_LOC_I") === STR_CURR_SITE_SEQ_I_DF("SITE_SEQ_I")).select($"DD9.*")
            val STR_CURR_SITE_SEQ_I_Clean_Unmatched_DF = Clean_Error_DF.except(STR_CURR_SITE_SEQ_I_DF_Join_Clean_DF)
            val STR_CURR_SITE_SEQ_I_Clean_Crunched_rdd = STR_CURR_SITE_SEQ_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val STR_CURR_SITE_SEQ_I_Clean_ErrorRecord = sqlContext.createDataFrame(STR_CURR_SITE_SEQ_I_Clean_Crunched_rdd, Error_Schema)
            val STR_CURR_SITE_SEQ_I_Clean_Error_Records = STR_CURR_SITE_SEQ_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: CO_LOC_I when mapped to STR_CURR.SITE_SEQ_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            STR_CURR_SITE_SEQ_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/STR_CURR_SITE")

           
            hdfs.mkdirs(new org.apache.hadoop.fs.Path(TempFolder + "/Error/ITEM_HIER_MDSE_ITEM"))
            val ITEM_HIER_MDSE_ITEM_REF_I_DF_Join_Clean_DF = Clean_Error_DF.as("DD13").join(ITEM_HIER_MDSE_ITEM_REF_I_DF.as("DD14"), Clean_Error_DF("MDSE_ITEM_REF_I") === ITEM_HIER_MDSE_ITEM_REF_I_DF("MDSE_ITEM_REF_I")).select($"DD13.*")
            val ITEM_HIER_MDSE_ITEM_REF_I_Clean_Unmatched_DF = Clean_Error_DF.except(ITEM_HIER_MDSE_ITEM_REF_I_DF_Join_Clean_DF)
            val ITEM_HIER_MDSE_ITEM_REF_I_Clean_Crunched_rdd = ITEM_HIER_MDSE_ITEM_REF_I_Clean_Unmatched_DF.rdd.map(t => Row(t.mkString(",")))
            val ITEM_HIER_MDSE_ITEM_REF_I_Clean_ErrorRecord = sqlContext.createDataFrame(ITEM_HIER_MDSE_ITEM_REF_I_Clean_Crunched_rdd, Error_Schema)
            val ITEM_HIER_MDSE_ITEM_REF_I_Clean_Error_Records = ITEM_HIER_MDSE_ITEM_REF_I_Clean_ErrorRecord.withColumn("Error_Description", lit("Refrential Integrity failed at column: MDSE_ITEM_REF_I when mapped to ITEM_HIER.MDSE_ITEM_REF_I")).withColumn("Data_Set_Name", lit(DataSetName)).withColumn("Insert_Time", current_timestamp()).select("Data_Set_Name", "Error_Description", "Error_Record", "Insert_Time")
            ITEM_HIER_MDSE_ITEM_REF_I_Clean_Error_Records.write.option("header", "true").option("escape", "\"").mode("append").csv(TempFolder + "/Error/ITEM_HIER_MDSE_ITEM")

          }
          try {
            val Error_Path_all = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File + "/AllError/*/*/*/*"
            val Error_PATH = ErrorPathRecord + "/" + Date + "/" + Error_Stage_File
          val ResDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").load(Error_Path_all)
          ResDF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Error_PATH)
          val listStatusReName_002 = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Error_PATH.replaceAll(adl_path, "") + "/part*.csv"))
          var path_002 = listStatusReName_002(0).getPath()
          val Current_Timestamp = CT.CurrentDate()
          sb.append(CT.CurrentTime() + "  : Renameing error record file at location : " + Error_PATH + "........\n")
          hdfs.rename(path_002, new org.apache.hadoop.fs.Path(Error_PATH.replaceAll(adl_path, "") + "/" + Enrich_File_Name.replaceAll(".csv", "") + "_" + Current_Timestamp + ".csv"))

        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
        }

        

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.Delete_File(hdfs, ErrorPathRecord + "/" + Date + "/" + Error_Stage_File + "/AllError")
        Delete_File.DeleteMultipleFile(adl_path, Stage_File, hadoopConf, hdfs)
        Delete_File.Delete_File(hdfs, Temp_Folder)
        res_flag = true
      } catch {
        case t: Throwable => {
          t.printStackTrace()
          sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          res_flag = false
        }
      } finally {
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }
      return res_flag
    }
}

