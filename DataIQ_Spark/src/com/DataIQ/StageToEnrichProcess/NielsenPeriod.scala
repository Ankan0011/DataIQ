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
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.coalesce
import org.apache.hadoop.conf.Configuration
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class NielsenPeriod extends java.io.Serializable {
  def IncrementNielsenPeriod(StageFile_url: String, EnrichedFile_url: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementNielsenPeriod \n")

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

        //Loading the Nielsen Period data from stage path
        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val nielsenPeriodData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile_url + "/*")
        val Stage_DF_Count = nielsenPeriodData.count().toString()

        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        val nielsenPeriodschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile_url)
        var nielsenPeriodsschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
        
        if (!nielsenPeriodschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile_url.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          println("urlStatus get Path:" + path_01)
          val path01 = path_01.toString()
          nielsenPeriodsschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        } 

        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")

        //Apply Date Transformation
        val Transform_Date_rdd = nielsenPeriodData.rdd.map(t => Row(t.get(0), t.get(1), t.get(2), CT.day(t.getString(3), "MM/dd/yy"), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8), t.get(9)))
        val Transform_Date_DF = sqlContext.createDataFrame(Transform_Date_rdd, nielsenPeriodData.schema)

        /**
         * The error record calculation start here
         */
        //Filter out error record from clean Data Frame
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = Transform_Date_DF.filter("SDESC == '' or DATE == '' or DATE == 'Unparceable' or DATE is null or SDESC is null or DATE == '0' or SDESC == '0'")

        val ErrorCol = "DATE,SDESC"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        //resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")
        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        //Clean DataFrame
        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        val Clean_DF_temp = Transform_Date_DF.filter("SDESC != '' and DATE != '' and DATE != 'Unparceable' and SDESC is not null and DATE is not null and SDESC != '0' and DATE != '0'")
        
        val Grouped_CleanDF = Clean_DF_temp.groupBy("DATE").agg(count("DATE").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)
        
        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("DATE") === Grouped_CleanDF("DATE")).select($"d1.*")
        
        //val Clean_DF = Clean_DF_temp.filter("SDESC != '' and DATE != '' and DATE != 'Unparceable' and SDESC is not null and DATE is not null and SDESC != '0' and DATE != '0'")
        val Enrich_Count = Clean_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n ")

        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, "DATE", Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
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
          val Clean_DF_join_Empty_Enrich_DF = nielsenPeriodsschemaDF.as("D1").join(Clean_DF.as("D2"), nielsenPeriodsschemaDF("DATE") === Clean_DF("DATE")).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = nielsenPeriodsschemaDF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val nielsenPeriodcleanFileIncr = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          
          //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)
          
          /**
           * New code end here
           */
          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
         // val nielsenPeriodcleanFileIncr = nielsenPeriodsschemaDF.union(Clean_DF).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile_url, nielsenPeriodcleanFileIncr, nielsenPeriodschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile_url, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenPeriod.IncrementNielsenPeriod - "+"Processed")
          sb.append(CT.CurrentTime() + " : NielsenPeriod.IncrementNielsenPeriod - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenPeriod.IncrementNielsenPeriod - "+"Rejected")
          sb.append(CT.CurrentTime() + " : NielsenPeriod.IncrementNielsenPeriod - Rejected \n")
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
      } finally {
        sb.append(CT.CurrentTime() + "  : IncrementNielsenPeriod method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}