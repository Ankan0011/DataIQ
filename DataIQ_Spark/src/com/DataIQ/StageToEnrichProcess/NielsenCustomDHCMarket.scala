package com.DataIQ.StageToEnrichProcess

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, LongType }
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{ FileStatus, FileSystem, Path, PathFilter }
import org.apache.hadoop.io.Writable
import java.net.URI
import org.apache.spark.sql.functions.udf
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
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

class NielsenCustomDHCMarket extends java.io.Serializable {

  def IncrementNielsenCustomDHCMarket(StageFile: String, EnrichedFile: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, Enrich_File_Name: String, IncrementFileName: String, Incremental_Folder_Path: String,FolderPath_temp:String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg:LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: IncrementNielsenCustomDHCMarket \n")
      
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

        // Generate the schema based on the string of schema
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        // Loads the file from the stage path
        sb.append(CT.CurrentTime() + " : Loading Stage_File_DF \n")
        val nielsenDHCMktschema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = nielsenDHCMktschema.count().toString()

        // Loads the already processed file from the Enriched folder
        sb.append(CT.CurrentTime() + " : Stage_File_DF loaded \n Enrich dataframe reading started.... \n")
        val nielsenDHCMktschemaDFCheck = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(EnrichedFile)

        // Create the Empty DF with the specified schema of dataset to check if the file in Enriched folder exists or not 
        var nielsenDHCMktschemaDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        // Condition to check if the loaded file from Enriched folder contains data or not
        if (!nielsenDHCMktschemaDFCheck.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          // Load the file from Enriched, if it contains the data
          nielsenDHCMktschemaDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }
        
        sb.append(CT.CurrentTime() + " : Enrich dataframe loaded\n ")
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation started...\n ")
        val Harmonic_Error_DF = nielsenDHCMktschema.filter("MKT == '' or MKT is null or MKT == '0' or LDESC == '' or LDESC is null or LDESC == '0'")

        val ErrorCol = "MKT,LDESC"
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")
        //resError_Count = HarmonicError_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonic Error segregation completed...\n ")
        
        sb.append(CT.CurrentTime() + " : Harmonization and validation process started...\n ")
        // Filter DF with key Fields as NOT Null as clean records
        val nielsenScanDHCMktschemaDFcleanFile_temp = nielsenDHCMktschema.filter("MKT !='' and MKT is not null and MKT !='0' and LDESC !='' and LDESC is not null and LDESC !='0'")
        
        val Clean_DF_temp = nielsenScanDHCMktschemaDFcleanFile_temp.withColumn("LDESC", upper(col("LDESC"))).withColumn("MKT", upper(col("MKT")))
        
        val Grouped_CleanDF = Clean_DF_temp.groupBy("MKT","LDESC").agg(count("MKT").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)
        
        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp("MKT") === Grouped_CleanDF("MKT") && Clean_DF_temp("LDESC") === Grouped_CleanDF("LDESC")).select($"d1.*")
        
        val Enrich_Count = Clean_DF.count().toString()
        sb.append(CT.CurrentTime() + " : Harmonization and validation process completed...\n ")
         
        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
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
          val Clean_DF_join_Empty_Enrich_DF = nielsenDHCMktschemaDF.as("D1").join(Clean_DF.as("D2"), nielsenDHCMktschemaDF("MKT") === Clean_DF("MKT") && nielsenDHCMktschemaDF("LDESC") === Clean_DF("LDESC")).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = nielsenDHCMktschemaDF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          
          val nielsenScanDHCMktDFcleanFileIncr = Combined_Clean_DF.withColumn("LDESC", upper(col("LDESC"))).withColumn("MKT", upper(col("MKT")))
          
          //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)
          
          /**
           * New code end here
           */

          // Create final DF with the union of DF from Stage file as well as with the file already in Enriched folder
         // val nielsenScanDHCMktDFcleanFileIncr_temp = (nielsenDHCMktschemaDF.union(nielsenScanDHCMktschemaDFcleanFile)).distinct()
         // val nielsenScanDHCMktDFcleanFileIncr = nielsenScanDHCMktDFcleanFileIncr_temp.withColumn("LDESC", upper(col("LDESC")))

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : BigFile file writing started...\n ")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(EnrichedFile, nielsenScanDHCMktDFcleanFileIncr, nielsenDHCMktschemaDFCheck, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : BigFile file writing completed...\n ")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenCustomDHCMarket.IncrementNielsenCustomDHCMarket - "+"Processed")
          sb.append(CT.CurrentTime() + " : NielsenCustomDHCMarket.IncrementNielsenCustomDHCMarket - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, Enrich_Count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "NielsenCustomDHCMarket.IncrementNielsenCustomDHCMarket - "+"Rejected")
          sb.append(CT.CurrentTime() + " : NielsenCustomDHCMarket.IncrementNielsenCustomDHCMarket - Rejected \n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + " : Exception occured while processing: " + t.getMessage + " \n")
          }
      }finally{
        sb.append(CT.CurrentTime() + "  : IncrementNielsenCustomDHCMarket method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}