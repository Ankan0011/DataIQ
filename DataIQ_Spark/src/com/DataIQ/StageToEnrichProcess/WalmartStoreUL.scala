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

class WalmartStoreUL extends java.io.Serializable {

  def IncrementWalmartStoreUL(StageFile_url: String, EnrichedFile_url: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, adl_path: String, schemaString: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementWalmartStoreUL \n")

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

        /* Loading the incremental dataset   .option("mode", "DROPMALFORMED")*/
        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val walmartStoreData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile_url + "/*")
        val Stage_DF_Count = walmartStoreData.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        val walmartStoreschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile_url)
        var walmartStoreschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        if (!walmartStoreschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          walmartStoreschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        } 
        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")

        val removeComma = udf((comma: String) => (comma.replaceAll(",", "")))
        val WalmartItemschemaComma = walmartStoreData.withColumn("STORE_KEY", removeComma(walmartStoreData("STORE_KEY"))).alias("STORE_KEY")

        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = WalmartItemschemaComma.filter("RETAILER_KEY == '' or RETAILER_KEY is null or RETAILER_KEY == '0' or STORE_KEY == '' or STORE_KEY is null  or STORE_KEY == '0'")

        val ErrorCol = "RETAILER_KEY, STORE_KEY"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        //resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")

        // Filter DF with key Fields as NOT Null as clean records
        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DF_temp = WalmartItemschemaComma.filter("RETAILER_KEY !='' and RETAILER_KEY is not null and RETAILER_KEY != '0' and STORE_KEY !='' and STORE_KEY is not null and STORE_KEY != '0'")
        
        val Grouped_CleanDF = Clean_DF_temp.groupBy("STORE_KEY").agg(count("STORE_KEY").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)
        
        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("STORE_KEY") === Grouped_CleanDF("STORE_KEY")).select($"d1.*")

        
        val Enrich_Count = Clean_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n ")
        
        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, "STORE_KEY", Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = Combined_Error_DF.count().toString()

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Incremental file writing started...\n ")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Incremental file writing completed...\n ")

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = walmartStoreschemaDF.as("D1").join(Clean_DF.as("D2"), walmartStoreschemaDF("STORE_KEY") === Clean_DF("STORE_KEY")).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = walmartStoreschemaDF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          /**
           * New code end here
           */
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
          //val Combined_Clean_DF = (walmartStoreschemaDF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile_url, Combined_Clean_DF, walmartStoreschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }
        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile_url, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "WalmartStoreUL.IncrementWalmartStoreUL - "+"Processed")
          sb.append(CT.CurrentTime() + " : WalmartStoreUL.IncrementWalmartStoreUL - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "WalmartStoreUL.IncrementWalmartStoreUL - "+"Rejected")
          sb.append(CT.CurrentTime() + " : WalmartStoreUL.IncrementWalmartStoreUL - Rejected \n")
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
        sb.append(CT.CurrentTime() + "  : IncrementWalmartStoreUL method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}

