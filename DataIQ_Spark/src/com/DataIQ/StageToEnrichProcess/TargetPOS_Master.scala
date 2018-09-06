package com.DataIQ.StageToEnrichProcess

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class TargetPOS_Master extends java.io.Serializable {

  def IncrementTargetMaster(StageFile: String, Enrich_File: String, Enrich_File_Name: String, Incremental_Folder_Path: String, IncrementFileName: String, key_Field: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, FolderPath_temp: String): Unit =
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
        val Stage_DF_Count = Stage_File_DF.count()

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
        val Clean_DF_temp = Stage_File_DF.filter(key_Field + " != '' and " + key_Field + " is not null and " + key_Field + " != '0'")

        val Grouped_CleanDF = Clean_DF_temp.groupBy(key_Field).agg(count(key_Field).as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)

        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp(key_Field) === Grouped_CleanDF(key_Field)).select($"d1.*")
        val Enrich_Count = Clean_DF.count()
        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = (Stage_DF_Count - Enrich_Count).toString()

        sb.append(CT.CurrentTime() + " : Harmonization and validation completed. \n")
        

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = Empty_Enrich_DF.as("D1").join(Clean_DF.as("D2"), Empty_Enrich_DF(key_Field) === Clean_DF(key_Field)).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = Empty_Enrich_DF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)

          /**
           * New code end here
           */
          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          //val Combined_Clean_DF = (Empty_Enrich_DF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write started.... \n")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(Enrich_File, Combined_Clean_DF, Enrich_DF, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count, Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Master.IncrementTargetMaster - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count, Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
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
  
  
  def IncrementTargetGSTDemo(StageFile: String, Enrich_File: String, Enrich_File_Name: String, Incremental_Folder_Path: String, IncrementFileName: String, key_Field: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, FolderPath_temp: String): Unit =
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
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("mode", "permissive").option("delimiter","|").option("escape", "\"").schema(schema).load(StageFile + "/*")
        val Stage_DF_Count = Stage_File_DF.count()

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
        val Clean_DF_temp = Stage_File_DF.filter(key_Field + " != '' and " + key_Field + " is not null and " + key_Field + " != '0'")

        val Grouped_CleanDF = Clean_DF_temp.groupBy(key_Field).agg(count(key_Field).as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)

        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), Clean_DF_temp(key_Field) === Grouped_CleanDF(key_Field)).select($"d1.*")
        val Enrich_Count = Clean_DF.count()
        /**
         * Error for duplicate start here
         */
        val Duplicate_Error = Clean_DF_temp.except(Clean_DF)
        //val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")
        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = (Stage_DF_Count - Enrich_Count).toString()

        sb.append(CT.CurrentTime() + " : Harmonization and validation completed. \n")
        

        if (!Enrich_Count.equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = Empty_Enrich_DF.as("D1").join(Clean_DF.as("D2"), Empty_Enrich_DF(key_Field) === Clean_DF(key_Field)).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = Empty_Enrich_DF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)

          /**
           * New code end here
           */
          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          //val Combined_Clean_DF = (Empty_Enrich_DF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write started.... \n")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(Enrich_File, Combined_Clean_DF, Enrich_DF, adl_path, Enrich_File_Name, hadoopConf, hdfs)
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count, Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Master.IncrementTargetMaster - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count, Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
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
  

  def IncrementTargetGRMMaster(StageFile: String, Enrich_File: String, Enrich_File_Name: String, Incremental_Folder_Path: String, IncrementFileName: String, key_Field1: String, key_Field2: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, schemaString: String, adl_path: String, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: TargetPOS_Master.IncrementTargetMaster \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = 0l
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
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").option("mode", "permissive").option("escape", "\"").schema(schema).load(StageFile + "/*/*")
        val Stage_DF_Count = Stage_File_DF.count()
        println("Stage_DF Count: " + Stage_DF_Count)

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
        val Harmonic_Error_DF = Stage_File_DF.filter(key_Field1 + " == '' or " + key_Field1 + " is null or " + key_Field1 + " == '0' or " + key_Field2 + " == '' or " + key_Field2 + " is null or " + key_Field2 + " == '0'")
        println("Harmonic Error DF: " + Harmonic_Error_DF.count().toString())
        val ErrorCol = key_Field1 + "," + key_Field2
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")

        sb.append(CT.CurrentTime() + " : Harmonic error calculation completed. \n")
        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        sb.append(CT.CurrentTime() + " : Harmonization and validation started to filter key fields not null. \n")
        //Clean DataFrame
        val Clean_DF_temp = Stage_File_DF.filter(key_Field1 + " != '' and " + key_Field1 + " is not null and " + key_Field1 + " != '0' and " + key_Field2 + " != '' and " + key_Field2 + " is not null and " + key_Field2 + " != '0'")

        val Grouped_CleanDF = Clean_DF_temp.groupBy(key_Field1, key_Field2).agg(count("*").as("COUNT_KeyField")).where(col("COUNT_KeyField") === 1)

        val Clean_DF = Clean_DF_temp.as("d1").join(Grouped_CleanDF.as("d2"), (Clean_DF_temp(key_Field1) === Grouped_CleanDF(key_Field1)) && (Clean_DF_temp(key_Field2) === Grouped_CleanDF(key_Field2))).select($"d1.*")

        /**
         * Error for duplicate start here
         */
        val Duplicate_Error_rdd = Clean_DF_temp.rdd.subtract(Clean_DF.rdd)
        val Duplicate_Error = sqlContext.createDataFrame(Duplicate_Error_rdd, schema)
        val Duplicate_Error_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Duplicate_Error, sqlContext, adl_path, hadoopConf, hdfs, "duplicate")

        sb.append(CT.CurrentTime() + " : Harmonization and validation completed. \n")
        val Enrich_Count = Clean_DF.count()

        //duplicate
        val Combined_Error_DF = Duplicate_Error_DF.union(HarmonicError_DF)
        resError_Count = Stage_DF_Count - Enrich_Count

        if (!Enrich_Count.toString().equals("0")) {
          sb.append(CT.CurrentTime() + " : Enrich Increment File write started.... \n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(Clean_DF, IncrementFileName, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)

          /**
           * New Code start here
           */
          val Clean_DF_join_Empty_Enrich_DF = Empty_Enrich_DF.as("D1").join(Clean_DF.as("D2"), (Empty_Enrich_DF(key_Field1) === Clean_DF(key_Field1)) && (Empty_Enrich_DF(key_Field2) === Clean_DF(key_Field2))).select($"D1.*")
          val Empty_Enrich_DF_Exp_rdd = Empty_Enrich_DF.rdd.subtract(Clean_DF_join_Empty_Enrich_DF.rdd)
          val Empty_Enrich_DF_Exp = sqlContext.createDataFrame(Empty_Enrich_DF_Exp_rdd, schema)
          val Combined_Clean_DF = (Empty_Enrich_DF_Exp.union(Clean_DF)).distinct()
          //val Combined_Clean_DF = Combined_Clean_DF_temp.dropDuplicates(key_Field)

          /**
           * New code end here
           */
          sb.append(CT.CurrentTime() + " : Enrich Increment File write completed. \n")
          //val Combined_Clean_DF = (Empty_Enrich_DF.union(Clean_DF)).distinct()

          //Write File in Enrich folder
          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write started.... \n")
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          flag = EnrichWrite.Enrich_Write(Enrich_File, Combined_Clean_DF, Enrich_DF, adl_path, Enrich_File_Name, hadoopConf, hdfs)

          sb.append(CT.CurrentTime() + " : Enrich BigFile folder File write completed. \n")
        }

        ErrorCal.Error_Path(Error_Stage_File, Combined_Error_DF, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)

        val Delete_File: DeleteFile = new DeleteFile()
        //Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)
        Delete_File.Delete_File(hdfs, StageFile)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count.toString(), resError_Count.toString(), Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "TargetPOS_Master.IncrementTargetMaster - "+"Processed")
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: " + Stage_DF_Count.toString() + ", Rejected : " + resError_Count.toString() + ", Output : " + Enrich_Count.toString() + "\n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count.toString(), resError_Count.toString(), Enrich_Count.toString(), StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
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

