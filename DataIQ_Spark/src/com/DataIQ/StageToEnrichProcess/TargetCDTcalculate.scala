package com.DataIQ.StageToEnrichProcess

import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import java.net.URI
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import com.DataIQ.ReferentialIntegrity.RefrentialValidationDF
import com.DataIQ.ReferentialIntegrity.RefrentialValidationErrorDF
import org.apache.hadoop.fs.FileSystem
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.hive.HiveContext
import com.DataIQ.Resource.EnrichIncrementWrite
import com.DataIQ.Resource.EnrichFileWrite
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class TargetCDTcalculate extends java.io.Serializable {

  def IncrementalTarget(Incremental_Folder_Path: String, Enrich_File: String, Stage_File: String, TargetPOS_Product: String, Enrich_File_Name: String, adl_path: String, schemaString: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext, col_Name: String,FolderPath_temp:String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg:LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: TargetCDTcalculate.IncrementalTarget \n")
      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = Stage_File.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        /**
         * This method will do the harmonization logic, refrential logic and then write to Enrich folder
         */
        import sqlContext.implicits._

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Enrich_File))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Enrich_File))
        }

        //Create the Schema from SchemaString
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        sb.append(CT.CurrentTime() + "  :  Reading all stage files at location : "+Stage_File+".......\n")
        //Load the TargetPOS_Sales Stage file into DataFtame
        val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").schema(schema).load(Stage_File + "/*")
        val Stage_DF_Count_temp = Stage_File_DF.count()
        val Stage_DF_Count = Stage_DF_Count_temp.toString()
        sb.append(CT.CurrentTime() + "  :  Reading stage file is complete.\n")

        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Product.......\n")
        //Load the TargetPOS_Product file into DataFrame, select only one column :- "ProductNbr" as we require only this column
        val Master_ProductNbr_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("escape", "\"").load(TargetPOS_Product).select("ProductNbr").distinct()
        sb.append(CT.CurrentTime() + "  :  Reading master file : TargetPOS_Product is complete.\n")
        
        sb.append(CT.CurrentTime() + "  :  Reading enriched file from folder : "+Enrich_File+"\n")
        //Load the TargetPOS_Sales Enrich file into DataFtame
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(schema).option("mode", "permissive").option("escape", "\"").load(Enrich_File)

        sb.append(CT.CurrentTime() + "  :  Reading enriched file is complete.\n")
        
        //Create Empty Enrich DataFrame
        var Empty_Enrich_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

        //This if block will act only when we already have data in Enrich Folder
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/*.csv"))
          var path_01 = listStatus(0).getPath()
          val path01 = path_01.toString()
          // Load the file from Enriched, if it contains the data
          Empty_Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").schema(schema).load(path01)
        }

        val removehyphen = udf((hyphen: String) => (hyphen.replaceAll("-", "")))

        val Hypen_DF = Stage_File_DF.withColumn(col_Name, removehyphen(Stage_File_DF(col_Name)))

        val Clean_DF_temp = Hypen_DF.withColumn(col_Name, lpad(Hypen_DF(col_Name), 9, "0"))

        /**
         * The error record calculation start here
         */
        //Filter out error record from clean Data Frame
        sb.append(CT.CurrentTime() + "  :  Calculating Harmonic_Error_DF........\n")
        val Harmonic_Error_DF = Clean_DF_temp.filter(col_Name + " == '' or " + col_Name + " is null or " + col_Name + " == '0'")

        val ErrorCol = col_Name
        val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
        val HarmonicError_DF = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")

        /**
         * The calculation for pushing clean DataFrame to Enrich start here
         */
        //Clean DataFrame
        sb.append(CT.CurrentTime() + "  :  Calculating Clean_DF........\n")
        val Clean_DF = Clean_DF_temp.filter(col_Name + " != '' and " + col_Name + " is not null and " + col_Name + " != '0'")

        //The following code is for Refrential Integrity
        sb.append(CT.CurrentTime() + "  :  Doing referential integrity at column 'ProductNbr'......\n")
        val RefInn: RefrentialValidationDF = new RefrentialValidationDF()
        val ProductNbr_Check = RefInn.CompareDataframe(Clean_DF, Master_ProductNbr_DF, col_Name, "ProductNbr", sc, sqlContext)

        val EnrichCount_temp = ProductNbr_Check.count()
        val EnrichCount = EnrichCount_temp.toString()
        //Write Incremental File
        if (!EnrichCount.equals("0")) {
          sb.append(CT.CurrentTime() + "  :  Writing increment file and renaming.......\n")
          val EnrichW: EnrichIncrementWrite = new EnrichIncrementWrite()
          EnrichW.Enrich_Write(ProductNbr_Check, Enrich_File_Name, Incremental_Folder_Path, adl_path, sqlContext, hadoopConf, hdfs)
        }

        //The following code is for Error
        sb.append(CT.CurrentTime() + "  :  Doing referential error integrity at column 'ProductNbr'...... \n")
        val RefIn: RefrentialValidationErrorDF = new RefrentialValidationErrorDF()
        val ProductNbrError_Check = RefIn.CompareErrorDataframe(Clean_DF, Master_ProductNbr_DF, col_Name, "ProductNbr", sc, sqlContext)

        //Combine Error Data Frame 
        val Error_DF = ProductNbrError_Check.distinct()

        //Write File in Enrich folder
        if (!EnrichCount.equals("0")) {
          val Combined_Clean_DF = (Empty_Enrich_DF.union(ProductNbr_Check)).distinct()
          val EnrichWrite: EnrichFileWrite = new EnrichFileWrite()
          sb.append(CT.CurrentTime() + "  :  Writing big file and renaming...... \n")
          flag = EnrichWrite.Enrich_Write(Enrich_File, Combined_Clean_DF, Enrich_DF, adl_path, Enrich_File_Name, hadoopConf, hdfs)
        }

        //Write File in Error Folder
        sb.append(CT.CurrentTime() + "  :  Calculating referentila error ...... \n")
        val RefErrorCheck = ErrorCal.CalculateError(Enrich_File_Name, ErrorCol, Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "referential")
        val resError = (HarmonicError_DF.union(RefErrorCheck)).distinct()
        ErrorCal.Error_Path(Error_Stage_File, resError, Enrich_File_Name, adl_path, sc, sqlContext, hdfs, hadoopConf)
        resError_Count = (Stage_DF_Count_temp-EnrichCount_temp).toString()

        sb.append(CT.CurrentTime() + "  :  Deleting files from path : "+Stage_File+"...... \n")
        val Delete_File: DeleteFile = new DeleteFile()
        Delete_File.DeleteMultipleFile(adl_path, Stage_File, hadoopConf, hdfs)

        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          
          sb.append(CT.CurrentTime() + "  : File is Processed and Input: "+Stage_DF_Count.toString()+", Rejected : "+resError_Count.toString()+", Output : "+EnrichCount.toString()+"\n")
          
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          
          sb.append(CT.CurrentTime() + "  : File is Rejected and Input: "+Stage_DF_Count.toString()+", Rejected : "+resError_Count.toString()+", Output : "+EnrichCount.toString()+"\n")
          
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + "  : Exception occurred while processing the file, Exception : "+t.getMessage+"\n")
          }
      }
      finally{
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), sb.toString())
      }

    }
}