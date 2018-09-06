package com.DataIQ.StageToEnrichProcess

import org.apache.spark.sql.types.{ StructType, StructField, StringType, LongType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.coalesce
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }
import org.apache.hadoop.io.Writable
import java.net.URI
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, SQLContext }

import org.apache.spark.sql.Column
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.hive.HiveContext
import java.sql.Timestamp
import java.text.SimpleDateFormat
import com.DataIQ.ReferentialIntegrity.RefrentialValidationDF
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.ReferentialIntegrity.RefrentialValidationErrorDF
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class PEA extends java.io.Serializable {
  def IncrementPEA(StageFile_url: String, EnrichedFile_url: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, adl_path: String, schemaString: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, Mapping_UPC_PPG: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementPEA \n")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile_url.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        import sqlContext.implicits._

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(EnrichedFile_url))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(EnrichedFile_url))
        }

        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        // Loading the PEA data from stage file
        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val PEAData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile_url + "/*")
        val Stage_DF_Count = PEAData.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        val PEAschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile_url)
       
        var PEAsschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")
        
        if (!PEAschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          PEAsschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        } 

        //Loading the Mapping UPC->PPG file from the enriched path
        sb.append(CT.CurrentTime() + " : Master File(s) loading....\n Loading Mapping_UPC_PPG file \n")
        val Master_Mapping_UPC_PPG = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("escape", "\"").load(Mapping_UPC_PPG).select("PEA_PPG").distinct()
        sb.append(CT.CurrentTime() + " : Master File loading completed \n ")

        //Apply Date Transformation
        val Transform_Date_rdd = PEAData.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), t.get(3), CT.day(t.getString(4).toString(), "yyyy-MM-dd"), CT.day(t.getString(5).toString(), "yyyy-MM-dd"), t.get(6), t.get(7), t.get(8), CT.day(t.getString(9).toString(), "yyyy-MM-dd"), CT.day(t.getString(10).toString(), "yyyy-MM-dd"), t.get(11), t.get(12), t.get(13), t.get(14), t.get(15), t.get(16), t.get(17), t.get(18), t.get(19), t.get(20), t.get(21), t.get(22), t.get(23), t.get(24), t.get(25), t.get(26), t.get(27), t.get(28), t.get(29), t.get(30), t.get(31), t.get(32), t.get(33), t.get(34), t.get(35), t.get(36), t.get(37), t.get(38), t.get(39), t.get(40), t.get(41), t.get(42), t.get(43), t.get(44), t.get(45), t.get(46), t.get(47), t.get(48), t.get(49), t.get(50), t.get(51), t.get(52), t.get(53), t.get(54), t.get(55), t.get(56), t.get(57), t.get(58), t.get(59), t.get(60), t.get(61), t.get(62), t.get(63), t.get(64), t.get(65), t.get(66), t.get(67), t.get(68), t.get(69), t.get(70), t.get(71), t.get(72), t.get(73), t.get(74), t.get(75), t.get(76), t.get(77), t.get(78), t.get(79), t.get(80), t.get(81), t.get(82), t.get(83), t.get(84), t.get(85), t.get(86), t.get(87), t.get(88), t.get(89), t.get(90), t.get(91), t.get(92), t.get(93), t.get(94), t.get(95), t.get(96), CT.day(t.getString(97).toString(), "yyyy-MM-dd"), t.get(98)))
        val Clean_DF_temp = sqlContext.createDataFrame(Transform_Date_rdd, PEAData.schema)

        /**
         * The error record calculation start here
         */
        //Filter out error record from clean Data Frame
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = Clean_DF_temp.filter("Retailer == '' or PPG == '' or Retailer is null or PPG is null or Retailer == '0' or PPG == '0'")

        val ErrorCol = "Retailer,PPG"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")

        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DF = Clean_DF_temp.filter("Retailer != '' and PPG != '' and Retailer is not null and PPG is not null  and Retailer != '0' and PPG != '0'")
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n Referential integrity validation started....\n")

        //The following code is for Refrential Integrity
        val RefInn: RefrentialValidationDF = new RefrentialValidationDF()
        val STORE_Check = RefInn.CompareDataframe(Clean_DF, Master_Mapping_UPC_PPG, "PPG", "PEA_PPG", sc, sqlContext)
        val EnrichCount = STORE_Check.count().toString()
        sb.append(CT.CurrentTime() + " : Referential integrity validation completed.... \n")

        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + " : Incremental file writing started...\n ")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(STORE_Check, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Incremental file writing completed...\n ")
        }

        //The following code is for Error
        val RefIn: RefrentialValidationErrorDF = new RefrentialValidationErrorDF()
        val Store_Error_Check = RefIn.CompareErrorDataframe(Clean_DF, Master_Mapping_UPC_PPG, "PPG", "PEA_PPG", sc, sqlContext)

        //Combine Error Data Frame 
        sb.append(CT.CurrentTime() + " : Failed Referential Validation calculation started...\n ")
        val Error_DF = Store_Error_Check.distinct()
        sb.append(CT.CurrentTime() + " : Failed Referential Validation calculation completed...\n ")

        //Write File in Enrich folder
        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
          val Combined_Clean = (PEAsschemaDF.union(STORE_Check)).distinct()
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile_url, Combined_Clean, PEAschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }

        sb.append(CT.CurrentTime() + " : Failed Referential Validation error file is being written...\n ")
        val RefErrorCheck = ErrorCal.CalculateError(Enrich_File_Name, "PPG", Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "referential")
        val resError = (HarmonicError_DF.union(RefErrorCheck)).distinct()
        ErrorCal.Error_Path(Error_Stage_File, resError, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)
        sb.append(CT.CurrentTime() + " : Failed Referential Validation error file writting is completed ...\n ")
        resError_Count = resError.count().toString()

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile_url, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "PEA.IncrementPEA - "+"Processed")
          sb.append(CT.CurrentTime() + " : PEA.IncrementPEA - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "PEA.IncrementPEA - "+"Rejected")
          sb.append(CT.CurrentTime() + " : PEA.IncrementPEA - Rejected \n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : IncrementPEA method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}