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
import java.sql.Timestamp
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class TDLinx extends java.io.Serializable {

  def IncrementTDLinx(StageFile: String, EnrichedFile: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementTDLinx \n")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        import sqlContext.implicits._

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(EnrichedFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(EnrichedFile))
        }
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        def emptyToNullString(c: Column) = when(length(trim(c)) > 0, c).otherwise(null)

        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val TDLinkDataTemp = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = TDLinkDataTemp.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        val TDlinkschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile)
        
        var TDLinkschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")

        if (!TDlinkschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          println("urlStatus get Path:" + path_01)
          val path01 = path_01.toString()
          TDLinkschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        } 
        
        val TDLinkDataTemp02 = TDLinkDataTemp.withColumnRenamed("Retailer Store Number", "Retailer_Store_Number").withColumnRenamed("ACV - Est. Yearly", "ACV_Est_Yearly")
        val TDLinkData = TDLinkDataTemp02.withColumn("Retailer_Store_Number", emptyToNullString(TDLinkDataTemp02("Retailer_Store_Number")).alias("Retailer_Store_Number"))

        //Filter out error record from clean Data Frame
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = TDLinkData.filter("TDLinx == '' or Retailer_Store_Number == '' or ACV_Est_Yearly == '' or Latitude == '' or Longitude == '' or TDLinx is null or Retailer_Store_Number is null or ACV_Est_Yearly is null or Latitude is null or Longitude is null or TDLinx == '0' or Retailer_Store_Number == '0' or ACV_Est_Yearly =='0' or Latitude =='0' or Longitude =='0'")

        val ErrorCol = "TDLinx,Retailer_Store_Number,ACV_Est_Yearly,Latitude, Longitude"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")

        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DF_temp = TDLinkData.filter("TDLinx != '' and Retailer_Store_Number != '' and ACV_Est_Yearly != '' and Latitude != '' and Longitude != '' and TDLinx is not null and Retailer_Store_Number is not null and ACV_Est_Yearly is not null and Latitude is not null and Longitude is not null and TDLinx != '0' and Retailer_Store_Number != '0' and ACV_Est_Yearly !='0' and Latitude !='0' and Longitude !='0'")

        val Grouped_CleanDF = Clean_DF_temp.groupBy("TDLinx").agg(count("TDLinx").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)
        
        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("TDLinx") === Grouped_CleanDF("TDLinx")).select($"d1.*")

        val Enrich_Count = Clean_DF.count().toString()
        
        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, "TDLinx", Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = Combined_Error_DF.count().toString()

        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n ")

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Incremental file writing started...\n ")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Incremental file writing completed...\n ")

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = TDLinkschemaDF.as("D1").join(Clean_DF.as("D2"), TDLinkschemaDF("TDLinx") === Clean_DF("TDLinx")).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = TDLinkschemaDF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          /**
           * New code end here
           */
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
          //val TDLinxcleanFileIncr = (TDLinkschemaDF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile, Combined_Clean_DF, TDlinkschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TDLinx.IncrementTDLinx - "+"Processed")
          sb.append(CT.CurrentTime() + " : TDLinx.IncrementTDLinx - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TDLinx.IncrementTDLinx - "+"Rejected")
          sb.append(CT.CurrentTime() + " : TDLinx.IncrementTDLinx - Rejected \n")
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
        sb.append(CT.CurrentTime() + "  : IncrementTDLinx method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }

}