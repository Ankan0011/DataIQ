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
import java.sql.Timestamp
import org.apache.spark.sql.{ SparkSession, SQLContext }
import org.apache.spark.sql.functions.coalesce
import org.apache.hadoop.conf.Configuration
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class NielsenProduct extends java.io.Serializable {

  def IncrementNielsenProduct(StageFile_url: String, EnrichedFile_url: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementNielsenProduct \n")

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

        //Load the Nielsen Product file from Stage path
        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val Stage_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile_url + "/*.csv")

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")

        var Empty_Enriched_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
        try {
          val Enriched_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").load(EnrichedFile_url)
          sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")
          Empty_Enriched_DF = Empty_Enriched_DF.unionAll(Enriched_DF)

        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }

        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = Stage_DF.filter("PRDC_KEY == '' or PRDC_KEY is null or PRDC_KEY == '0' or PRDC_TAG == '' or PRDC_TAG is null or PRDC_TAG == '0' or PRDC_CODE == '' or PRDC_CODE is null or PRDC_CODE == '0'")
        //val Harmonic_Error_DF = Stage_DF.filter("PRDC_KEY == '' or PRDC_KEY is null or PRDC_KEY == '0' or PRDC_TAG == '' or PRDC_TAG is null or PRDC_TAG == '0'")

        val ErrorCol = "PRDC_KEY,PRDC_TAG,PRDC_CODE"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
       
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")

        // Filter DF with key Fields as NOT Null as clean records
        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DFtemp = Stage_DF.filter("PRDC_KEY != '' and PRDC_TAG != '' and PRDC_CODE != '' and PRDC_KEY is not null and PRDC_TAG is not null and PRDC_CODE is not null and PRDC_KEY != '0' and PRDC_TAG != '0' and PRDC_CODE != '0'")
        //val Clean_DFtemp = Stage_DF.filter("PRDC_KEY != '' and PRDC_TAG != '' and PRDC_KEY is not null and PRDC_TAG is not null and PRDC_KEY != '0' and PRDC_TAG != '0'")
        val Clean_DF_temp = Clean_DFtemp.withColumn("PRDC_CODE", lpad(Clean_DFtemp("PRDC_CODE"), 13, "0"))
        val Grouped_CleanDF = Clean_DF_temp.groupBy("PRDC_TAG", "PRDC_CODE").agg(count("PRDC_TAG").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)

        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("PRDC_TAG") === Grouped_CleanDF("PRDC_TAG") && Clean_DF_temp("PRDC_CODE") === Grouped_CleanDF("PRDC_CODE")).select($"d1.*")

        val Enrich_Count = Clean_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n ")

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Incremental file writing started...\n ")
          // val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          // EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
          try {
            if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Incremental_Folder_Path))) {
              hdfs.mkdirs(new org.apache.hadoop.fs.Path(Incremental_Folder_Path))

            }
            Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Incremental_Folder_Path)
            sb.append(CT.CurrentTime() + " : Writing incremental part file complete.....\n ")
            val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Incremental_Folder_Path.replaceAll(adl_path, "") + "/part*.csv"))
            var path_04 = listStatusReName(0).getPath()
            hdfs.rename(path_04, new org.apache.hadoop.fs.Path(Incremental_Folder_Path.replaceAll(adl_path, "") + "/" + IncrementFileName))
            sb.append(CT.CurrentTime() + " : Renameing part file in incremental folder complete.....\n ")
            sb.append(CT.CurrentTime() + " : Incremental file writing completed...\n ")
          } catch {
            case t: Throwable => t.printStackTrace() // TODO: handle error
          }

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = Empty_Enriched_DF.as("DD1").join(Clean_DF.as("DD2"), Empty_Enriched_DF("PRDC_TAG") === Clean_DF("PRDC_TAG") && Empty_Enriched_DF("PRDC_CODE") === Clean_DF("PRDC_CODE")).select($"DD1.*")
          //val Empty_Enrich_DF_Exp = Empty_Enriched_DF.except(Clean_DF_join_Empty_Enrich_DF)
          val Empty_Enrich_DF_Exp_rdd = Empty_Enriched_DF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()

         //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)

          /**
           * New code end here
           */
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
          //val nielsenProductcleanFileIncr = (nielsenProductschemaDF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          /*val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile_url, nielsenProductcleanFileIncr, nielsenProductschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          */
          try {
            Combined_Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(EnrichedFile_url)

            //Delete the existing file in Enrich Folder
            try {
              val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
              var path_02 = listStatus(0).getPath()
              hdfs.delete(path_02)
            } catch {
              case t: Throwable => t.printStackTrace() // TODO: handle error
            }

            //The following line will rename the enrich file
            val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/part*.csv"))
            var path_04 = listStatusReName(0).getPath()
            flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
            sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
          } catch {
            case t: Throwable => t.printStackTrace() // TODO: handle error
          }

        }

        /**
         * Error for duplicate start here
         */
        sb.append(CT.CurrentTime() + " : Duplicate error calculation started...\n ")
        //val Duplicate_Error = Clean_DF_temp.except(Clean_DF)
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, "PRDC_TAG,PRDC_CODE", Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = Combined_Error_DF.count().toString()
        
        sb.append(CT.CurrentTime() + " : resError_Count : "+resError_Count+"\n ")
        
         sb.append(CT.CurrentTime() + " : Writing error record file...\n ")
        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)
        sb.append(CT.CurrentTime() + " : Writing error record file is complete...\n ")
        val Stage_DF_Count = Stage_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Stage_DF_Count : "+Stage_DF_Count+"\n ")
        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile_url, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenProduct.IncrementNielsenProduct - "+"Processed")
          sb.append(CT.CurrentTime() + " : NielsenProduct.IncrementNielsenProduct - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenProduct.IncrementNielsenProduct - "+"Rejected")
          sb.append(CT.CurrentTime() + " : NielsenProduct.IncrementNielsenProduct - Rejected \n")
        }
      } catch {
        case t: Throwable =>
          {
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenProduct.IncrementNielsenProduct - " + t.getMessage)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : " + t.getMessage + "\n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : IncrementNielsenProduct method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}
