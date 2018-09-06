package com.DataIQ.StageToEnrichProcess

import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import java.net.URI
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import com.DataIQ.ReferentialIntegrity.RefrentialValidationDF
import com.DataIQ.ReferentialIntegrity.RefrentialValidationErrorDF
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class StoreToRegion extends java.io.Serializable {

  def IncrementalStore2Region(EnrichFile: String, StageFile: String, FileSchema: String, EnrichFileName: String, IncrementFileName: String, IncrementFolderPath: String, TDLinks_MasterEnrich_File: String, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext,FolderPath_temp:String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg:LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: StoreToRegion.IncrementalStore2Region \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {

        val col_Name = "TDLinx"
        /**
         * This method will do the harmonization logic, refrential logic and then write to Enrich folder
         */
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

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Loading Master_TDLinx_DF \n")
        //Load the TDLinks_MasterEnrich_File file into DataFrame, select only one column :- "TDLinx" as we require only this column
        val Master_TDLinx_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(TDLinks_MasterEnrich_File).select("TDLinx").distinct()

        sb.append(CT.CurrentTime() + " : Master_TDLinx_DF loaded \n Enrich dataframe reading started.... \n")
        //Load the TargetPOS_Sales Enrich file into DataFtame
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("mode", "permissive").option("escape", "\"").load(EnrichFile)

        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded. \n")
        //Create Empty Enrich DataFrame
        var Empty_Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        //This if block will act only when we already have data in Enrich Folder
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichFile.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          // Load the file from Enriched, if it contains the data
          sb.append(CT.CurrentTime() + " : Load the file from Enriched, if it contains the data. \n")
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }

        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        //The following code is for Refrential Integrity
        sb.append(CT.CurrentTime() + " : Referential integrity validation started.... \n")
        val RefInn: RefrentialValidationDF = new RefrentialValidationDF()
        val TDLinx_Clean = RefInn.CompareDataframe(Stage_File_DF, Master_TDLinx_DF, col_Name, "TDLinx", sc, sqlContext)
        val EnrichCount = TDLinx_Clean.count().toString()
        sb.append(CT.CurrentTime() + " : Referential integrity validation completed.... \n")

        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(TDLinx_Clean, IncrementFileName, IncrementFolderPath, adl_path, sqlContext, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
        }

        //The following code is for Error
        sb.append(CT.CurrentTime() + " : Referential integrity validation error calculation started.... \n")
        val RefIn: RefrentialValidationErrorDF = new RefrentialValidationErrorDF()
        val TDLinx_Check = RefIn.CompareErrorDataframe(Stage_File_DF, Master_TDLinx_DF, col_Name, "TDLinx", sc, sqlContext)
        sb.append(CT.CurrentTime() + " : Referential integrity validation error calculation completed. \n")

        //Write File in Enrich folder
        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder write started.... \n")
          val Combined_Clean_DF = (Empty_Enrich_DF.union(TDLinx_Clean)).distinct()
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichFile, Combined_Clean_DF, Enrich_DF, adl_path, EnrichFileName, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder write completed. \n")
        }
        
        //Write File in Error Folder
        sb.append(CT.CurrentTime() + " : Referential error write started in error folder. \n")
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val RefErrorCheck = ErrorCal.CalculateError(EnrichFileName, col_Name, TDLinx_Check, sqlContext, adl_path, hadoopConf, hdfs, "referential")
        ErrorCal.Error_Path(Error_Stage_File, RefErrorCheck, EnrichFileName, adl_path, sc, sqlContext, hdfs, hadoopConf)
        resError_Count = RefErrorCheck.count().toString()
        sb.append(CT.CurrentTime() + " : Referential error write completed in error folder. \n")

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "Store2Region.IncrementalStore2Region - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "Store2Region.IncrementalStore2Region - "+"Rejected")
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + EnrichCount.toString() + "\n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : StoreToRegion.IncrementalStore2Region method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), sb.toString())
      }
    }
}