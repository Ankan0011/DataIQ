package com.DataIQ.StageToEnrichProcess

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, LongType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, LongType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.coalesce
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }
import org.apache.hadoop.io.Writable
import java.net.URI
import org.apache.spark.sql.{ SparkSession, SQLContext }
import com.DataIQ.Resource.Property
import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.coalesce
import org.apache.hadoop.conf.Configuration
import scala.util.Try
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class PriceList extends java.io.Serializable {
  def IncrementalPricelist(EnrichFile: String, StageFile: String, FileSchema: String, EnrichFileName: String, IncrementFileName: String, IncrementFolderPath: String, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: PriceList.IncrementalPricelist \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        /**
         * This method will do the harmonization logic, refrential logic and then write to Enrich folder
         */
        val col_Name = "UNIT_UPC"
        import sqlContext.implicits._

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(EnrichFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(EnrichFile))
        }

        //Create the Schema from SchemaString
        val fields = FileSchema.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        //Load the TargetPOS_Sales Stage file into DataFtame
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = Stage_File_DF.count().toString()

        //Load the Nielsen_Product_EnrichFile file into DataFrame, select only one column :- "PRDC_CODE" as we require only this column
        //val Master_PRDC_CODE_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").load(Nielsen_Product_EnrichFile).select("PRDC_CODE").distinct()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        //Load the TargetPOS_Sales Enrich file into DataFtame
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(EnrichFile)

        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded. \n")
        //Create Empty Enrich DataFrame
        var Empty_Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        //This if block will act only when we already have data in Enrich Folder
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichFile.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
         
          sb.append(CT.CurrentTime() + " : Load the file from Enriched, if it contains the data. \n")
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }

        
        sb.append(CT.CurrentTime() + "  :  Doing date transform and adding column 'Milli_Count' ie milisecond count.....\n")
        //Apply Date Transformation
        val Transform_Date_rdd = Stage_File_DF.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9), t.get(10), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27), t.get(28), t.get(29), t.get(30), t.get(31), t.get(32), CT.day(t.getString(33).toString(), "yyyyMMdd"), t.get(34), t.get(35), t.get(36), t.get(37), t.get(38), t.get(39), t.get(40), t.get(41), t.get(42), t.get(43), t.get(44), t.get(45)))
        val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, schema)

        /**
         * The error record calculation start here
         */
        sb.append(CT.CurrentTime() + " : Harmonic error calculation started for filteration of key fields null.... \n")
        val Harmonic_Error_DF = Clean_DF_temp.filter(col_Name + " == '' or " + col_Name + " is null or " + col_Name + " == '0'")
        val ErrorCol = "UNIT_UPC"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(EnrichFileName, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic error calculation completed. \n")

        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        //Clean DataFrame
        sb.append(CT.CurrentTime() + " : Harmonization and validation started to filter key fields not null. \n")
        val Clean_DFNotNull = Clean_DF_temp.filter(col_Name + " != '' and " + col_Name + " is not null and " + col_Name + " != '0'")

        sb.append(CT.CurrentTime() + " : left padding on UNIT_UPC field upto 13 characters. \n")
        val Clean_DF = Clean_DFNotNull.withColumn("UNIT_UPC", lpad(Clean_DFNotNull("UNIT_UPC"), 13, "0"))
        val Enrich_Count = Clean_DF.count().toString()

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, IncrementFolderPath, adl_path, sqlContext, hadoopConf, hdfs)

          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          val Combined_Clean_DF_temp = Empty_Enrich_DF.union(Clean_DF)
          val Combined_Clean_DF = Combined_Clean_DF_temp.distinct()
          
          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write started.... \n")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichFile, Combined_Clean_DF, Enrich_DF, adl_path, EnrichFileName, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, HarmonicError_DF, EnrichFileName, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "Pricelist.IncrementalPricelist - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "Pricelist.IncrementalPricelist - "+"Rejected")
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "Pricelist.IncrementalPricelist - "+t.getMessage)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : PriceList.IncrementalPricelist method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), sb.toString())
      }
    }
}