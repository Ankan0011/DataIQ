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
import org.apache.spark.sql.functions.coalesce
import org.apache.hadoop.conf.Configuration
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

class NielsenDemographic extends java.io.Serializable{

  def IncrementDemographic(StageFile_url: String, EnrichedFile_url: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, adl_path: String, schemaString: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, TDLinx_Enriched: String,FolderPath_temp:String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg:LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementDemographic \n")
      
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
        
        sb.append(CT.CurrentTime() + " : Loading EnrichedFile \n")
        val demoCountschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile_url)
        var demoCountschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        if (!demoCountschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          println("urlStatus get Path:" + path_01)
          val path01 = path_01.toString()
          demoCountschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }
        
        sb.append(CT.CurrentTime() + " : EnrichedFile loaded \n Stage File loading is started... \n")
        
        //Loading the Demographic data from stage file   
        val demoCountData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile_url + "/*")
        val Stage_DF_Count = demoCountData.count().toString()
        sb.append(CT.CurrentTime() + " : Stage File loaded \n ")

        //Loading the TDLinx file from the enriched path
        sb.append(CT.CurrentTime() + " : Master File(s) loading....\n Loading TDLinx \n")
        val TDLinx_Data = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("escape", "\"").load(TDLinx_Enriched).select("TDLinx").distinct()

        //Filter out error record from clean Data Frame
        sb.append(CT.CurrentTime() + " : Master File loading completed \n Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = demoCountData.filter("TDLinx == '' or STORE_NAME == '' or Latitude == '' or Longitude == '' or TDLinx is null or STORE_NAME is null or Latitude is null or Longitude is null or TDLinx == '0' or STORE_NAME == '0' or Latitude =='0' or Longitude =='0'")

        val ErrorCol = "TDLinx,STORE_NAME,Latitude,Longitude"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")
        
        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DF = demoCountData.filter("TDLinx != '' or STORE_NAME != '' or Latitude != '' or Longitude != '' or TDLinx is not null or STORE_NAME is not null or Latitude is not null or Longitude is not null or TDLinx != '0' or STORE_NAME != '0' or Latitude !='0' or Longitude !='0'")
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n Referential integrity validation started....\n")

        //The following code is for Refrential Integrity
        val RefInn: RefrentialValidationDF = new RefrentialValidationDF()
        val TDLinx_Check = RefInn.CompareDataframe(Clean_DF, TDLinx_Data, "TDLinx", "TDLinx", sc, sqlContext)
        val EnrichCount = TDLinx_Check.count().toString()
        sb.append(CT.CurrentTime() + " : Referential integrity validation completed.... \n")
        
        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + " : Incremental file writing started...\n ")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(TDLinx_Check, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Incremental file writing completed...\n ")
        }

        //The following code is for Error
        sb.append(CT.CurrentTime() + " : Failed Referential Validation calculation started...\n ")
        val RefIn: RefrentialValidationErrorDF = new RefrentialValidationErrorDF()
        val Store_Error_Check = RefIn.CompareErrorDataframe(Clean_DF, TDLinx_Data, "TDLinx", "TDLinx", sc, sqlContext)
        

        //Combine Error Data Frame 
        val Error_DF = Store_Error_Check.distinct()
        sb.append(CT.CurrentTime() + " : Failed Referential Validation calculation completed...\n ")
        
        if (!EnrichCount.equals("0")) {
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
          val nielsenDemocleanFileIncr = (demoCountschemaDF.union(TDLinx_Check)).distinct()
          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile_url, nielsenDemocleanFileIncr, demoCountschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }

        sb.append(CT.CurrentTime() + " : Failed Referential Validation error file is being written...\n ")
        val RefErrorCheck = ErrorCal.CalculateError(Enrich_File_Name, "TDLinx", Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "referential")
        val resError = (HarmonicError_DF.union(RefErrorCheck)).distinct()
        ErrorCal.Error_Path(Error_Stage_File, resError, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)
        resError_Count = resError.count().toString()
        sb.append(CT.CurrentTime() + " : Failed Referential Validation error file writting is completed ...\n ")

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile_url, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenDemographic.IncrementDemographic - "+"Processed")
          sb.append(CT.CurrentTime() + " : NielsenDemographic.IncrementDemographic - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenDemographic.IncrementDemographic - "+"Rejected")
          sb.append(CT.CurrentTime() + " : NielsenDemographic.IncrementDemographic - Rejected \n")
        }

      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + " : Exception occured while processing : " + t.getMessage + " \n")
          }
      }finally{
        sb.append(CT.CurrentTime() + "  : IncrementDemographic method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}