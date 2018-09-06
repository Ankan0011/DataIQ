package com.DataIQ.StageToEnrichProcess
/**
 * This Code is for Walmart Assortment
 */
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import com.DataIQ.ReferentialIntegrity.RefrentialValidationDF
import com.DataIQ.ReferentialIntegrity.RefrentialValidationErrorDF
import com.DataIQ.Resource.DeleteFile
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class AssortmentCalculation extends java.io.Serializable {

  def IncrementCalculate(EnrichFile: String, StageFile: String, Walmart_Item: String, Walmart_Store: String, KeyField: String, adl_path: String, sqlContext: SQLContext, sc: SparkContext, hadoopConf: Configuration, hdfs: FileSystem, FolderPath_temp: String): Unit =
    {
      val CT: CurrentDateTime = new CurrentDateTime()
      val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()

      
      var EnrichFileName = ""
      var Stage_DF_Count_temp = 0L
      var Stage_DF_Count = ""
      var EnrichCount_temp = 0L
      var EnrichCount = ""
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: AssortmentCalculation.IncrementCalculate.......\n")
      
      var resError_Count = ""
      var StartTime = ""
      var EndTime = ""
      var flag = false
      val Error_Stage_File = StageFile.replaceAll("/Stage", "").replaceAll(adl_path, "")

      try {
        import sqlContext.implicits._
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(StageFile.replaceAll(adl_path, "") + "/*.csv"))
        if (!listStatus.isEmpty) {
          
          sb.append(CT.CurrentTime() + "  :  Reading master file : Walmart_Item......\n")
          //Load the TargetPOS_Product file into DataFrame, select only one column :- "PRDC_TAG" as we require only this column
          val Master_walmartITEM_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(Walmart_Item).select("UPC").distinct()

          sb.append(CT.CurrentTime() + "  :  Reading master file : Walmart_Store......\n")
          //Load the TargetPOS_Location file into DataFrame, select only one column :- "MKT" as we require only this column 
          val Master_walmartSTORE_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").load(Walmart_Store).select("STORE_KEY").distinct()

          if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(EnrichFile))) {
            hdfs.mkdirs(new org.apache.hadoop.fs.Path(EnrichFile))
          }

          sb.append(CT.CurrentTime() + "  :  Going inside FOR loop......\n")
          for (i <- 0 to (listStatus.length - 1)) {
            StartTime = CT.CurrentTime()
            val Stage_File_Ind = listStatus.apply(i).getPath.toString()
            EnrichFileName = Stage_File_Ind.split("/").last

            sb.append(CT.CurrentTime() + "  :  Reading Stage file : "+Stage_File_Ind+"......\n")
            //Load the TargetPOS_Sales Stage file into DataFtame
            val Stage_File_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("inferSchema", "false").load(Stage_File_Ind)
            Stage_DF_Count_temp = Stage_File_DF.count()
            Stage_DF_Count = Stage_DF_Count_temp.toString()

            //Harmonic error calculation
            val Harmonic_Error_DF = Stage_File_DF.where(Stage_File_DF.col("Store_Nbr") === "" || Stage_File_DF.col("Store_Nbr").isNull ||
              Stage_File_DF.col("Planogram_Name") === "" || Stage_File_DF.col("Planogram_Name").isNull ||
              Stage_File_DF.col("FileName") === "" || Stage_File_DF.col("FileName").isNull ||
              Stage_File_DF.col("Send_Trait") === "" || Stage_File_DF.col("Send_Trait").isNull ||
              Stage_File_DF.col("Planogram_Height") === "" || Stage_File_DF.col("Planogram_Height").isNull ||
              Stage_File_DF.col("Planogram_Width") === "" || Stage_File_DF.col("Planogram_Width").isNull ||
              Stage_File_DF.col("UPC") === "" || Stage_File_DF.col("UPC").isNull ||
              Stage_File_DF.col("PL_Name") === "" || Stage_File_DF.col("PL_Name").isNull ||
              Stage_File_DF.col("PL_Category") === "" || Stage_File_DF.col("PL_Category").isNull ||
              Stage_File_DF.col("PL_Subcategory") === "" || Stage_File_DF.col("PL_Subcategory").isNull ||
              Stage_File_DF.col("Merch_Group") === "" || Stage_File_DF.col("Merch_Group").isNull ||
              Stage_File_DF.col("PL_Brand") === "" || Stage_File_DF.col("PL_Brand").isNull ||
              Stage_File_DF.col("PL_Size") === "" || Stage_File_DF.col("PL_Size").isNull || Stage_File_DF.col("PL_Size") < "0" ||
              Stage_File_DF.col("PL_UOM") === "" || Stage_File_DF.col("PL_UOM").isNull ||
              Stage_File_DF.col("PL_Height") === "" || Stage_File_DF.col("PL_Height").isNull || Stage_File_DF.col("PL_Height") < "0" ||
              Stage_File_DF.col("PL_Width") === "" || Stage_File_DF.col("PL_Width").isNull || Stage_File_DF.col("PL_Width") < "0" ||
              Stage_File_DF.col("PL_Depth") === "" || Stage_File_DF.col("PL_Depth").isNull || Stage_File_DF.col("PL_Depth") < "0" ||
              Stage_File_DF.col("PL_CaseTotalNumber") === "" || Stage_File_DF.col("PL_CaseTotalNumber").isNull || Stage_File_DF.col("PL_CaseTotalNumber") < "0" ||
              Stage_File_DF.col("Product_Case_total_number") === "" || Stage_File_DF.col("Product_Case_total_number").isNull || Stage_File_DF.col("Product_Case_total_number") < "0" ||
              Stage_File_DF.col("Product_Height") === "" || Stage_File_DF.col("Product_Height").isNull || Stage_File_DF.col("Product_Height") < "0" ||
              Stage_File_DF.col("Product_Width") === "" || Stage_File_DF.col("Product_Width").isNull || Stage_File_DF.col("Product_Width") < "0" ||
              Stage_File_DF.col("Product_Depth") === "" || Stage_File_DF.col("Product_Depth").isNull || Stage_File_DF.col("Product_Depth") < "0" ||
              Stage_File_DF.col("Position_Height") === "" || Stage_File_DF.col("Position_Height").isNull || Stage_File_DF.col("Position_Height") < "0" ||
              Stage_File_DF.col("Position_Width") === "" || Stage_File_DF.col("Position_Width").isNull || Stage_File_DF.col("Position_Width") < "0" ||
              Stage_File_DF.col("Position_Depth") === "" || Stage_File_DF.col("Position_Depth").isNull || Stage_File_DF.col("Position_Depth") < "0" ||
              Stage_File_DF.col("Position_DFacings") === "" || Stage_File_DF.col("Position_DFacings").isNull || Stage_File_DF.col("Position_DFacings") < "0" ||
              Stage_File_DF.col("Position_HFacings") === "" || Stage_File_DF.col("Position_HFacings").isNull || Stage_File_DF.col("Position_HFacings") < "0" ||
              Stage_File_DF.col("Position_Vfacings") === "" || Stage_File_DF.col("Position_Vfacings").isNull || Stage_File_DF.col("Position_Vfacings") < "0" ||
              Stage_File_DF.col("Position_X") === "" || Stage_File_DF.col("Position_X").isNull || Stage_File_DF.col("Position_X") < "0" ||
              Stage_File_DF.col("Position_Y") === "" || Stage_File_DF.col("Position_Y").isNull || Stage_File_DF.col("Position_Y") < "0")

            val ErrorCol = KeyField
            val ErrorCal: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            val HarmonicError_DF = ErrorCal.CalculateError(EnrichFileName, ErrorCol, Harmonic_Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "harmonic")

            //Clean data calculation start here
            val Clean_DF_Temp = Stage_File_DF.filter(Stage_File_DF.col("Store_Nbr") =!= "" && Stage_File_DF.col("Store_Nbr").isNotNull &&
              Stage_File_DF.col("Planogram_Name") =!= "" && Stage_File_DF.col("Planogram_Name").isNotNull &&
              Stage_File_DF.col("FileName") =!= "" && Stage_File_DF.col("FileName").isNotNull &&
              Stage_File_DF.col("Send_Trait") =!= "" && Stage_File_DF.col("Send_Trait").isNotNull &&
              Stage_File_DF.col("Planogram_Height") =!= "" && Stage_File_DF.col("Planogram_Height").isNotNull &&
              Stage_File_DF.col("Planogram_Width") =!= "" && Stage_File_DF.col("Planogram_Width").isNotNull &&
              Stage_File_DF.col("UPC") =!= "" && Stage_File_DF.col("UPC").isNotNull &&
              Stage_File_DF.col("PL_Name") =!= "" && Stage_File_DF.col("PL_Name").isNotNull &&
              Stage_File_DF.col("PL_Category") =!= "" && Stage_File_DF.col("PL_Category").isNotNull &&
              Stage_File_DF.col("PL_Subcategory") =!= "" && Stage_File_DF.col("PL_Subcategory").isNotNull &&
              Stage_File_DF.col("Merch_Group") =!= "" && Stage_File_DF.col("Merch_Group").isNotNull &&
              Stage_File_DF.col("PL_Brand") =!= "" && Stage_File_DF.col("PL_Brand").isNotNull &&
              Stage_File_DF.col("PL_Size") =!= "" && Stage_File_DF.col("PL_Size").isNotNull && Stage_File_DF.col("PL_Size") >= "0" &&
              Stage_File_DF.col("PL_UOM") =!= "" && Stage_File_DF.col("PL_UOM").isNotNull &&
              Stage_File_DF.col("PL_Height") =!= "" && Stage_File_DF.col("PL_Height").isNotNull && Stage_File_DF.col("PL_Height") >= "0" &&
              Stage_File_DF.col("PL_Width") =!= "" && Stage_File_DF.col("PL_Width").isNotNull && Stage_File_DF.col("PL_Width") >= "0" &&
              Stage_File_DF.col("PL_Depth") =!= "" && Stage_File_DF.col("PL_Depth").isNotNull && Stage_File_DF.col("PL_Depth") >= "0" &&
              Stage_File_DF.col("PL_CaseTotalNumber") =!= "" && Stage_File_DF.col("PL_CaseTotalNumber").isNotNull && Stage_File_DF.col("PL_CaseTotalNumber") >= "0" &&
              Stage_File_DF.col("Product_Case_total_number") =!= "" && Stage_File_DF.col("Product_Case_total_number").isNotNull && Stage_File_DF.col("Product_Case_total_number") >= "0" &&
              Stage_File_DF.col("Product_Height") =!= "" && Stage_File_DF.col("Product_Height").isNotNull && Stage_File_DF.col("Product_Height") >= "0" &&
              Stage_File_DF.col("Product_Width") =!= "" && Stage_File_DF.col("Product_Width").isNotNull && Stage_File_DF.col("Product_Width") >= "0" &&
              Stage_File_DF.col("Product_Depth") =!= "" && Stage_File_DF.col("Product_Depth").isNotNull && Stage_File_DF.col("Product_Depth") >= "0" &&
              Stage_File_DF.col("Position_Height") =!= "" && Stage_File_DF.col("Position_Height").isNotNull && Stage_File_DF.col("Position_Height") >= "0" &&
              Stage_File_DF.col("Position_Width") =!= "" && Stage_File_DF.col("Position_Width").isNotNull && Stage_File_DF.col("Position_Width") >= "0" &&
              Stage_File_DF.col("Position_Depth") =!= "" && Stage_File_DF.col("Position_Depth").isNotNull && Stage_File_DF.col("Position_Depth") >= "0" &&
              Stage_File_DF.col("Position_DFacings") =!= "" && Stage_File_DF.col("Position_DFacings").isNotNull && Stage_File_DF.col("Position_DFacings") >= "0" &&
              Stage_File_DF.col("Position_HFacings") =!= "" && Stage_File_DF.col("Position_HFacings").isNotNull && Stage_File_DF.col("Position_HFacings") >= "0" &&
              Stage_File_DF.col("Position_Vfacings") =!= "" && Stage_File_DF.col("Position_Vfacings").isNotNull && Stage_File_DF.col("Position_Vfacings") >= "0" &&
              Stage_File_DF.col("Position_X") =!= "" && Stage_File_DF.col("Position_X").isNotNull && Stage_File_DF.col("Position_X") >= "0" &&
              Stage_File_DF.col("Position_Y") =!= "" && Stage_File_DF.col("Position_Y").isNotNull && Stage_File_DF.col("Position_Y") >= "0")

            val Clean_DF = Clean_DF_Temp.withColumn("PL_Name", regexp_replace(Clean_DF_Temp.col("PL_Name"), "&", "and"))
              .withColumn("PL_Category", regexp_replace(Clean_DF_Temp.col("PL_Category"), " ", "_"))
              .withColumn("PL_Subcategory", regexp_replace(Clean_DF_Temp.col("PL_Subcategory"), " ", "_"))
              .withColumn("Merch_Group", regexp_replace(Clean_DF_Temp.col("Merch_Group"), " ", "_"))
              .withColumn("PL_Brand", regexp_replace(Clean_DF_Temp.col("PL_Brand"), "`", ""))

            sb.append(CT.CurrentTime() + "  :  Referential validation started......\n")
            val RefInn: RefrentialValidationDF = new RefrentialValidationDF()
            val STORE_Check = RefInn.CompareDataframe(Clean_DF, Master_walmartSTORE_DF, "Store_Nbr", "STORE_KEY", sc, sqlContext)
            val ITEM_Clean = RefInn.CompareDataframe(STORE_Check, Master_walmartITEM_DF, "UPC", "UPC", sc, sqlContext)

            sb.append(CT.CurrentTime() + "  :  Referential error validation started......\n")
            val RefIn: RefrentialValidationErrorDF = new RefrentialValidationErrorDF()
            val Store_Error_Check = RefIn.CompareErrorDataframe(Clean_DF, Master_walmartSTORE_DF, "Store_Nbr", "STORE_KEY", sc, sqlContext)
            val Item_Error_Clean = RefIn.CompareErrorDataframe(Clean_DF, Master_walmartITEM_DF, "UPC", "UPC", sc, sqlContext)

            val Error_DF_temp = Store_Error_Check.union(Item_Error_Clean)
            val Error_DF = Error_DF_temp.distinct()
            val RefErrorCheck = ErrorCal.CalculateError(EnrichFileName, "Store_Nbr,UPC", Error_DF, sqlContext, adl_path, hadoopConf, hdfs, "referential")

            val Combined_Clean_DF = ITEM_Clean.distinct()
            EnrichCount_temp = Combined_Clean_DF.count()
            EnrichCount = EnrichCount_temp.toString()

            //Write File in Enrich folder
            if (!EnrichCount.equals("0")) {
              sb.append(CT.CurrentTime() + "  :  Writing file to : "+EnrichFile+"......\n")
              Combined_Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(EnrichFile)
              val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichFile.replaceAll(adl_path, "") + "/part*.csv"))
              var path_04 = listStatusReName(0).getPath()
              sb.append(CT.CurrentTime() + "  :  Renameing file at : "+EnrichFile+"......\n")
              flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(EnrichFile.replaceAll(adl_path, "") + "/" + EnrichFileName))
            }
            //The following line will rename the enrich file

            val resError = (HarmonicError_DF.union(RefErrorCheck)).distinct()
            ErrorCal.Error_Path(Error_Stage_File, resError, EnrichFileName, adl_path, sc, sqlContext, hdfs, hadoopConf)
            resError_Count = (Stage_DF_Count_temp-EnrichCount_temp).toString()

            EndTime = CT.CurrentTime()
            if (flag) {

              ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Processed", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
              
              sb.append(CT.CurrentTime() + "  : File is Processed and Input: "+Stage_DF_Count.toString()+", Rejected : "+resError_Count.toString()+", Output : "+EnrichCount.toString()+"\n")
            } else {

              ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Rejected", Stage_DF_Count, resError_Count, EnrichCount, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
              
            sb.append(CT.CurrentTime() + "  : File is Rejected and Input: "+Stage_DF_Count.toString()+", Rejected : "+resError_Count.toString()+", Output : "+EnrichCount.toString()+"\n")
            }
          }
          sb.append(CT.CurrentTime() + "  :  Deleting file from : "+StageFile+"......\n")
          val Delete_File: DeleteFile = new DeleteFile()
          Delete_File.DeleteMultipleFile(adl_path, StageFile, hadoopConf, hdfs)

        }

      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(EnrichFileName, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            sb.append(CT.CurrentTime() + "  :  Exception Occured while Processing : "+t.getMessage+".......\n")
            
          }
      }
      finally{
        logg.InsertLog(adl_path, FolderPath_temp, EnrichFileName.replaceAll(".csv", ""), sb.toString())
      }
    }

}