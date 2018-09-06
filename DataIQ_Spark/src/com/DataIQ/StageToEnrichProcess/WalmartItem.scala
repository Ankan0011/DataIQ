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
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class WalmartItem extends java.io.Serializable {

  def IncrementWalmartItem(EnrichFile: String, StageFile: String, FileSchema: String, EnrichFileName: String, IncrementFileName: String, IncrementFolderPath: String, adl_path: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext,FolderPath_temp:String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg:LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: WalmartItem.IncrementWalmartItem \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {

        import sqlContext.implicits._

        // The parent path of adl (data lake store)
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf)
        
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(EnrichFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(EnrichFile))
        }
        // Generate the schema based on the string of schema
        val fields = FileSchema.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        sb.append(CT.CurrentTime() + " : Loading Stage_DF \n")
        // Loads the file from the stage path
        val Stage_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = Stage_DF.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_DF loaded \n Enrich dataframe reading started.... \n")
        // Loads the already processed file from the Enriched folder
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichFile)
        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded. \n")

        // Create the Empty DF with the specified schema of dataset to check if the file in Enriched folder exists or not 
        var Empty_Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        // Condition to check if the loaded file from Enriched folder contains data or not
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichFile.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          // Load the file from Enriched, if it contains the data
          sb.append(CT.CurrentTime() + " : Load the file from Enriched, if it contains the data. \n")
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("escape", "\"").load(path01)
        }

        sb.append(CT.CurrentTime() + " : udf to remove comma from 'ITEM_KEY' field. \n")
        //udf to remove comma from "ITEM_KEY" field
        val removeComma = udf((comma: String) => (comma.replaceAll(",", "")))
        val Clean_DFtemp = Stage_DF.withColumn("ITEM_KEY", removeComma(Stage_DF("ITEM_KEY"))).alias("ITEM_KEY")

        /**
         * The error record calculation start here
         */
        sb.append(CT.CurrentTime() + " : Harmonic error calculation started for filteration of key fields null.... \n")
        //Filter out error record from clean Data Frame
        val Harmonic_Error_DF = Clean_DFtemp.filter("ITEM_KEY == '' or UPC == '' or ITEM_KEY is null or UPC is null or ITEM_KEY == '0' or UPC == '0'")

        val ErrorCol = "ITEM_KEY, UPC"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(EnrichFileName, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        //resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic error calculation completed. \n")
        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        sb.append(CT.CurrentTime() + " : Harmonization and validation started to filter key fields not null. \n")
        //Clean DataFrame
        val Clean_DF_tranform = Clean_DFtemp.filter("ITEM_KEY != '' and UPC != ''and ITEM_KEY is not null and UPC is not null and ITEM_KEY != '0' and UPC != '0'")

        sb.append(CT.CurrentTime() + " : left padding on UPC field upto 13 characters. \n")
        val Clean_DF_temp = Clean_DF_tranform.withColumn("UPC", lpad(Clean_DF_tranform("UPC"), 13, "0")).alias("UPC")
        sb.append(CT.CurrentTime() + " : Harmonization and validation completed. \n")
        val Grouped_CleanDF = Clean_DF_temp.groupBy("ITEM_KEY").agg(count("ITEM_KEY").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)
        
        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("ITEM_KEY") === Grouped_CleanDF("ITEM_KEY")).select($"d1.*")
        
        val Enrich_Count = Clean_DF.count().toString()

        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(EnrichFileName, "ITEM_KEY", Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = Combined_Error_DF.count().toString()
        
        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, IncrementFolderPath, adl_path, sqlContext, hadoopConf, hdfs)

          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          
          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = Empty_Enrich_DF.as("D1").join(Clean_DF.as("D2"), Empty_Enrich_DF("ITEM_KEY") === Clean_DF("ITEM_KEY")).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = Empty_Enrich_DF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          /**
           * New code end here
           */
          
          //val Combined_Clean_DF = (Empty_Enrich_DF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write started.... \n")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichFile, Combined_Clean_DF, Enrich_DF, adl_path, EnrichFileName, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, EnrichFileName, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "WalmartItem.IncrementWalmartItem - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "WalmartItem.IncrementWalmartItem - "+"Rejected")
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            //logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), "WalmartItem.IncrementWalmartItem - "+t.getMessage)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : IncrementWalmartItem method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), sb.toString())
      }
    }
}