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
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.LogWrite

class ProcessCalculate extends java.io.Serializable {

  def IncrementalFeature(EnrichFile: String, StageFile: String, FileSchema: String, EnrichFileName: String, IncrementFileName: String, IncrementFolderPath: String, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: ProcessCalculate.IncrementalFeature \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = 0L
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
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
          // Load the file from Enriched, if it contains the data
          sb.append(CT.CurrentTime() + " : Load the file from Enriched, if it contains the data. \n")
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }
        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        if (!Stage_DF_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Stage_File_DF, IncrementFileName, IncrementFolderPath, adl_path, sqlContext, hadoopConf, hdfs)

          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          val Combined_Clean_DF = (Empty_Enrich_DF.union(Stage_File_DF)).distinct()

          sb.append(CT.CurrentTime() + " : Enrich BigFile folder write started.... \n")
          //Write File in Enrich folder
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichFile, Combined_Clean_DF, Enrich_DF, adl_path, EnrichFileName, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder write completed. \n")
        }

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Processed", Stage_DF_Count, "0", Stage_DF_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "ProcessCalculate.IncrementalFeature - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Processed : " + resError_Count.toString() + ", Output : " + Stage_DF_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Rejected", Stage_DF_Count, "0", Stage_DF_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "ProcessCalculate.IncrementalFeature - "+"Rejected")
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Stage_DF_Count.toString() + "\n")
        }

      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "ProcessCalculate.IncrementalFeature - "+t.getMessage)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : ProcessCalculate.IncrementalFeature method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), sb.toString())
      }
    }

  def IncrementWeather(StageFile: String, Enrich_File: String, Enrich_File_Name: String, Incremental_Folder_Path: String, IncrementFileName: String, key_Field: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: TargetPOS_Master.IncrementTargetMaster \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        import sqlContext.implicits._
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Enrich_File))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Enrich_File))
        }

        // Generate the schema based on the string of schema
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        //Load the TargetPOS_Sales Stage file into DataFtame
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = Stage_File_DF.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        //Load the TargetPOS_Sales Enrich file into DataFtame
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("mode", "permissive").option("escape", "\"").load(Enrich_File)

        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded. \n")
        var Empty_Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        //This if block will act only when we already have data in Enrich Folder
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          // Load the file from Enriched, if it contains the data
          sb.append(CT.CurrentTime() + " : Load the file from Enriched, if it contains the data. \n")
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }
        /**
         * The error record calculation start here
         */
        sb.append(CT.CurrentTime() + " : Harmonic error calculation started for filteration of key fields null.... \n")
        //Filter out error record from clean Data Frame
        val Harmonic_Error_DF = Stage_File_DF.filter(key_Field + " == '' or " + key_Field + " is null or " + key_Field + " == '0'")

        val ErrorCol = key_Field
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")

        sb.append(CT.CurrentTime() + " : Harmonic error calculation completed. \n")
        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        sb.append(CT.CurrentTime() + " : Harmonization and validation started to filter key fields not null. \n")
        //Clean DataFrame
        val Clean_DF = Stage_File_DF.filter(key_Field + " != '' and " + key_Field + " is not null and " + key_Field + " != '0'")

        val Combined_Error_DF = HarmonicError_DF
        resError_Count = Combined_Error_DF.count().toString()

        sb.append(CT.CurrentTime() + " : Harmonization and validation completed. \n")
        val Enrich_Count = Clean_DF.count().toString()

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          /*val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)*/
          try {

            if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Incremental_Folder_Path))) {
              hdfs.mkdirs(new org.apache.hadoop.fs.Path(Incremental_Folder_Path))

            }
            Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Incremental_Folder_Path)
            val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Incremental_Folder_Path.replaceAll(adl_path, "") + "/part*.csv"))
            var path_04 = listStatusReName(0).getPath()
            hdfs.rename(path_04, new org.apache.hadoop.fs.Path(Incremental_Folder_Path.replaceAll(adl_path, "") + "/" + IncrementFileName))
          } catch {
            case t: Throwable => {
              t.printStackTrace()
            }
          }

          val Combined_Clean_DF = (Empty_Enrich_DF.union(Clean_DF)).distinct()

          try {
            Combined_Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Enrich_File)

            //Delete the existing file in Enrich Folder
            if (!Enrich_DF.limit(1).rdd.isEmpty) {
              val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
              var path_02 = listStatus(0).getPath()
              hdfs.delete(path_02)
            }

            //The following line will rename the enrich file
            val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/part*.csv"))
            var path_04 = listStatusReName(0).getPath()
            flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))

          } catch {
            case t: Throwable => {
              t.printStackTrace()
              flag = false
            }
          }
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Master.IncrementTargetMaster - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Master.IncrementTargetMaster - "+"Rejected")
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
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
        sb.append(CT.CurrentTime() + "  : IncrementTargetMaster method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }
    }
}