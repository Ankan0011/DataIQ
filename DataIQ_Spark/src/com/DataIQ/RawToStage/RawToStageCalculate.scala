package com.DataIQ.RawToStage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.DataIQ.Resource.Validation
import com.DataIQ.Resource.DeleteFile
import java.net.URI
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import com.DataIQ.Resource.RawToStageError
import com.DataIQ.Resource.CurrentDateTime
import org.apache.log4j.Logger
import org.apache.log4j.LogManager

import com.DataIQ.Resource.LogWrite
import com.DataIQ.Resource.Property

object RawToStageCalculate {

  def main(args: Array[String]): Unit = {

    var input_parameter = ""
    //val ADL_Select = args(1)
    val CT: CurrentDateTime = new CurrentDateTime()
    val logg: LogWrite = new LogWrite()
    var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: RawToStageCalculate.main \n")

    val TablePush: RawToStageError = new RawToStageError()
    val del: DeleteFile = new DeleteFile()
    var RowCount = ""
    var StartTime = ""
    var EndTime = ""
    var Raw_File: String = ""
    val Prop: Property = new Property()

    sb.append(CT.CurrentTime() + " : Ascessing properties for spark_serializer, spark_executor_cores, spark_executor_instances, spark_yarn_containerLauncherMaxThreads\n")
    val spark_serializer = Prop.getProperty("spark_serializer")
    val spark_executor_cores = Prop.getProperty("spark_executor_cores")
    val spark_executor_instances = Prop.getProperty("spark_executor_instances")
    val spark_yarn_containerLauncherMaxThreads = Prop.getProperty("spark_yarn_containerLauncherMaxThreads")

    sb.append(CT.CurrentTime() + " : Properties are as follows:\n spark_serializer: " + spark_serializer + "\nspark_executor_cores: " + spark_executor_cores + " \n spark_executor_instances: " + spark_executor_instances + " \nspark_yarn_containerLauncherMaxThreads: " + spark_yarn_containerLauncherMaxThreads + " \n")

    var adl_path = ""

    var Raw_to_Stage_ErrorFile = ""
    var FolderPath_temp = ""

    val Sparkconf = new SparkConf().setAppName("RawToStageCalculate")
    Sparkconf.set("spark.serializer", spark_serializer)
    Sparkconf.set("spark.executor.cores", spark_executor_cores).set("spark.executor.instances", spark_executor_instances).set("spark.yarn.containerLauncherMaxThreads", spark_yarn_containerLauncherMaxThreads)
    val sc = new SparkContext(Sparkconf)
    val sqlContext = new SQLContext(sc)

    adl_path = args(1)
    sb.append(CT.CurrentTime() + " : adlPath/second parameter is : " + adl_path + "\n")

    var Error_Folder_Path = ""
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf)
    try {

      input_parameter = args(0)
      sb.append(CT.CurrentTime() + " : DatasetName/first parameter is : " + input_parameter + "\n")

      Raw_to_Stage_ErrorFile = adl_path + Prop.getProperty("Raw_to_Stage_ErrorFile")
      FolderPath_temp = adl_path + Prop.getProperty("Raw_To_Stage_LogInfo")
      sb.append(CT.CurrentTime() + " : Raw_to_Stage_ErrorFile is : " + Raw_to_Stage_ErrorFile + "\n")
      sb.append(CT.CurrentTime() + " : FolderPath_temp is : " + FolderPath_temp + "\n")

      val SpecialCharacter = Prop.getProperty("Special_Character")
      val SpecialChar: Array[String] = SpecialCharacter.split(",")

      var Stage_File: String = ""
      var Schema = ""
      var valid = 1
      var Delimited = ","

      input_parameter match {
        case "Nielsen_Scan_UPC" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_Scan_UPC\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Stage_File")
          Schema = Prop.getProperty("Nielsen_Scan_UPC_File_Schema")
        }
        case "Walmart_ICECREAM_ITEM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_ICECREAM_ITEM\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Stage_File")
          Schema = Prop.getProperty("Walmart_ICECREAM_ITEM_File_Schema")
        }
        case "Walmart_ULHairOil_ITEM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_ULHairOil_ITEM\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Stage_File")
          Schema = Prop.getProperty("Walmart_ULHairOil_ITEM_File_Schema")
        }
        case "Walmart_TEA_ITEM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_TEA_ITEM\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_TEA_ITEM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Walmart_TEA_ITEM_Stage_File")
          Schema = Prop.getProperty("Walmart_TEA_ITEM_File_Schema")
        }
        case "Walmart_SPREADS_ITEM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_SPREADS_ITEM\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Stage_File")
          Schema = Prop.getProperty("Walmart_SPREADS_ITEM_File_Schema")
        }
        case "Walmart_USASCC_ITEM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_USASCC_ITEM\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Stage_File")
          Schema = Prop.getProperty("Walmart_USASCC_ITEM_File_Schema")
        }
        case "8451_SHCD" => {
          sb.append(CT.CurrentTime() + " : Inside Case : 8451_SHCD\n")
          Raw_File = adl_path + Prop.getProperty("8451_SHCD_Raw_File")
          Stage_File = adl_path + Prop.getProperty("8451_SHCD_Stage_File")
          Schema = Prop.getProperty("8451_SHCD_File_Schema")
        }
        case "8451_Styling" => {
          sb.append(CT.CurrentTime() + " : Inside Case : 8451_Styling\n")
          Raw_File = adl_path + Prop.getProperty("8451_Styling_Raw_File")
          Stage_File = adl_path + Prop.getProperty("8451_Styling_Stage_File")
          Schema = Prop.getProperty("8451_Styling_File_Schema")
        }
        case "InfoScout_Trip" => {
          sb.append(CT.CurrentTime() + " : Inside Case : InfoScout_Trip\n")
          Raw_File = adl_path + Prop.getProperty("InfoScout_Trip_Raw_File")
          Stage_File = adl_path + Prop.getProperty("InfoScout_Trip_Stage_File")
          Schema = Prop.getProperty("InfoScout_Trip_File_Schema")
        }
        case "InfoScout_Feed" => {
          sb.append(CT.CurrentTime() + " : Inside Case : InfoScout_Feed\n")
          Raw_File = adl_path + Prop.getProperty("InfoScout_Feed_Raw_File")
          Stage_File = adl_path + Prop.getProperty("InfoScout_Feed_Stage_File")
          Schema = Prop.getProperty("InfoScout_Feed_File_Schema")
        }
        case "PriceList" => {
          sb.append(CT.CurrentTime() + " : Inside Case : PriceList\n")
          Raw_File = adl_path + Prop.getProperty("PriceList_Raw_File")
          Stage_File = adl_path + Prop.getProperty("PriceList_Stage_File")
          Schema = Prop.getProperty("PriceList_File_Schema")
        }
        case "WalmartCalendar" => {
          sb.append(CT.CurrentTime() + " : Inside Case : WalmartCalendar\n")
          Raw_File = adl_path + Prop.getProperty("WalmartCalendar_Raw_File")
          Stage_File = adl_path + Prop.getProperty("WalmartCalendar_Stage_File")
          Schema = Prop.getProperty("WalmartCalendar_File_Schema")
        }
        case "StoreRegionMapping" => {
          sb.append(CT.CurrentTime() + " : Inside Case : StoreRegionMapping\n")
          Raw_File = adl_path + Prop.getProperty("StoreRegionMapping_Raw_File")
          Stage_File = adl_path + Prop.getProperty("StoreRegionMapping_Stage_File")
          Schema = Prop.getProperty("StoreRegionMapping_File_Schema")
        }
        case "NielsenUPCtoPPGmapping" => {
          sb.append(CT.CurrentTime() + " : Inside Case : NielsenUPCtoPPGmapping\n")
          Raw_File = adl_path + Prop.getProperty("NielsenUPCtoPPGmapping_Raw_File")
          Stage_File = adl_path + Prop.getProperty("NielsenUPCtoPPGmapping_Stage_File")
          Schema = Prop.getProperty("NielsenUPCtoPPGmapping_File_Schema")
        }
        case "PeriodMappingGWTNCalendar" => {
          sb.append(CT.CurrentTime() + " : Inside Case : PeriodMappingGWTNCalendar\n")
          Raw_File = adl_path + Prop.getProperty("PeriodMappingGWTNCalendar_Raw_File")
          Stage_File = adl_path + Prop.getProperty("PeriodMappingGWTNCalendar_Stage_File")
          Schema = Prop.getProperty("PeriodMappingGWTNCalendar_File_Schema")
       }
        case "Holiday" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Holiday\n")
          Raw_File = adl_path + Prop.getProperty("Holiday_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Holiday_Stage_File")
          Schema = Prop.getProperty("Holiday_File_Schema")
        }
        case "TDLinx_Mapping" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TDLinx_Mapping\n")
          Raw_File = adl_path + Prop.getProperty("TDLinx_Mapping_Raw_File")
          Stage_File = adl_path + Prop.getProperty("TDLinx_Mapping_Stage_File")
          Schema = Prop.getProperty("TDLinx_Mapping_File_Schema")
        }
        case "TargetCDT_Personal_Wash" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetCDT_Personal_Wash\n")
          Raw_File = adl_path + Prop.getProperty("Target_CDT_PERSONAL_WASH_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_CDT_PERSONAL_WASH_Stage_File")
          Schema = Prop.getProperty("Target_CDT_PERSONAL_WASH_Schema")
        }
        case "TargetCDT_Hair" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetCDT_Hair\n")
          Raw_File = adl_path + Prop.getProperty("Target_CDT_HAIR_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_CDT_HAIR_Stage_File")
          Schema = Prop.getProperty("Target_CDT_HAIR_Schema")
        }
        case "TargetCDT_Deo" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetCDT_Deo\n")
          Raw_File = adl_path + Prop.getProperty("Target_CDT_DEO_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_CDT_DEO_Stage_File")
          Schema = Prop.getProperty("Target_CDT_DEO_Schema")
        }
        case "TargetCDT_Ice_Cream" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetCDT_Ice_Cream\n")
          Raw_File = adl_path + Prop.getProperty("Target_CDT_ICE_CREAM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_CDT_ICE_CREAM_Stage_File")
          Schema = Prop.getProperty("Target_CDT_ICE_CREAM_Schema")
        }
        case "TargetCDT_Tea" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetCDT_Tea\n")
          Raw_File = adl_path + Prop.getProperty("Target_CDT_TEA_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_CDT_TEA_Stage_File")
          Schema = Prop.getProperty("Target_CDT_TEA_Schema")
        }
        case "Feature_Vision" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Feature_Vision\n")
          Raw_File = adl_path + Prop.getProperty("Feature_Vision_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Feature_Vision_Stage_File")
          Schema = Prop.getProperty("Feature_Vision_Schema")
        }
        case "Nielsen_Elasticity" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_Elasticity\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_Elasticity_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_Elasticity_Stage_File")
          Schema = Prop.getProperty("Nielsen_Elasticity_Schema")
        }
        case "TargetPOS_Sales" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetPOS_Sales\n")
          Raw_File = adl_path + Prop.getProperty("Target_POS_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_POS_Sales_Stage_File")
          Schema = Prop.getProperty("TargetPOS_Sales_schemaString")
        }
        case "TargetPOS_Inventory" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetPOS_Inventory\n")
          Raw_File = adl_path + Prop.getProperty("Target_POS_INVEN_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_POS_inventory_Stage_File")
          Schema = Prop.getProperty("TargetPOS_inventory_schemaString")
        }
        case "TargetPOS_Location" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetPOS_Location\n")
          Raw_File = adl_path + Prop.getProperty("Target_Location_Master_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_Location_Master_Stage_File")
          Schema = Prop.getProperty("Target_Location_Master_Schema")
        }
        case "TargetPOS_Product" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TargetPOS_Product\n")
          Raw_File = adl_path + Prop.getProperty("Target_Product_Master_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_Product_Master_Stage_File")
          Schema = Prop.getProperty("Target_Product_Master_Schema")
        }
        case "Target_GRM_LOCATION" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Target_GRM_LOCATION\n")
          Raw_File = adl_path + Prop.getProperty("Target_GRM_LOCATION_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_GRM_LOCATION_Stage_File")
          Schema = Prop.getProperty("Target_GRM_LOCATION_Schema")
        }

        case "Target_GRM_PRODUCT" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Target_GRM_PRODUCT\n")
          Raw_File = adl_path + Prop.getProperty("Target_GRM_PRODUCT_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_GRM_PRODUCT_Stage_File")
          Schema = Prop.getProperty("Target_GRM_PRODUCT_Schema")
        }
        case "STR_CNCPT" => {
          sb.append(CT.CurrentTime() + " : Inside Case : STR_CNCPT\n")
          valid = 2
          Raw_File = adl_path + Prop.getProperty("STR_CNCPT_Raw_File")
          Stage_File = adl_path + Prop.getProperty("STR_CNCPT_Stage_File")
          Schema = Prop.getProperty("STR_CNCPT_Schema")
          Delimited = "|"
        }
        case "GST_DEMOS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : GST_DEMOS\n")
          valid = 2
          Raw_File = adl_path + Prop.getProperty("GST_DEMOS_Raw_File")
          Stage_File = adl_path + Prop.getProperty("GST_DEMOS_Stage_File")
          Schema = Prop.getProperty("GST_DEMOS_Schema")
          Delimited = "|"
        }
        case "STR_CURR" => {
          sb.append(CT.CurrentTime() + " : Inside Case : STR_CURR\n")
          valid = 2
          Raw_File = adl_path + Prop.getProperty("STR_CURR_Raw_File")
          Stage_File = adl_path + Prop.getProperty("STR_CURR_Stage_File")
          Schema = Prop.getProperty("STR_CURR_Schema")
          Delimited = "|"
        }
        case "ITEM_HIER" => {
          sb.append(CT.CurrentTime() + " : Inside Case : ITEM_HIER\n")
          valid = 2
          Raw_File = adl_path + Prop.getProperty("ITEM_HIER_Raw_File")
          Stage_File = adl_path + Prop.getProperty("ITEM_HIER_Stage_File")
          Schema = Prop.getProperty("ITEM_HIER_Schema")
          Delimited = "|"
        }

        case "Target_GRM" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Target_GRM\n")
          valid = 2
          Raw_File = adl_path + Prop.getProperty("Target_GRM_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Target_GRM_Stage_File")
          Schema = Prop.getProperty("Target_GRM_Schema")
          Delimited = "|"
        }
        case "Weather" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Weather\n")
          Raw_File = adl_path + Prop.getProperty("Weather_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Weather_Stage_File")
          Schema = Prop.getProperty("Weather_Schema")
        }
        case "PEA_PPG_Mapping" => {
          sb.append(CT.CurrentTime() + " : Inside Case : PEA_PPG_Mapping\n")
          Raw_File = adl_path + Prop.getProperty("PEA_PPG_MAP_Raw")
          Stage_File = adl_path + Prop.getProperty("PEA_PPG_MAP_Stage")
          Schema = Prop.getProperty("PEA_PPG_MAP_Schema")
        }
        case "Walmart_Assortment" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Assortment\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_Assortment_Raw")
          Stage_File = adl_path + Prop.getProperty("Walmart_Assortment_Stage")
          Schema = Prop.getProperty("Walmart_Assortment_Schema")
        }
        case "Walmart_Store_HairOil" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Store_HairOil\n")
          Raw_File = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_Raw_File")
          Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_Stage_File")
          Schema = Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_Schema")
        }
        case "Walmart_Store_SASC" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Store_SASC\n")
          Raw_File = adl_path + Prop.getProperty("WalmartStoreUL_SASC_STORE_Raw_File")
          Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_SASC_STORE_Stage_File")
          Schema = Prop.getProperty("WalmartStoreUL_SASC_STORE_Schema")
        }
        case "Walmart_Store_Tea" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Store_Tea\n")
          Raw_File = adl_path + Prop.getProperty("WalmartStoreUL_TEA_STORE_Raw_file")
          Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_TEA_STORE_Stage_File")
          Schema = Prop.getProperty("WalmartStoreUL_TEA_STORE_Schema")
        }
        case "Walmart_Store_Spreads" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Store_Spreads\n")
          Raw_File = adl_path + Prop.getProperty("WalmartStoreUL_SPREADS_STORE_Raw_file")
          Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_SPREADS_STORE_Stage_File")
          Schema = Prop.getProperty("WalmartStoreUL_SPREADS_STORE_Schema")
        }
        case "Walmart_Store_IceCream" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Store_IceCream\n")
          Raw_File = adl_path + Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_Raw_file")
          Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_Stage_File")
          Schema = Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_Schema")
        }
        case "PEA" => {
          sb.append(CT.CurrentTime() + " : Inside Case : PEA\n")
          Raw_File = adl_path + Prop.getProperty("PEA_Raw_file")
          Stage_File = adl_path + Prop.getProperty("PEA_Stage_File")
          Schema = Prop.getProperty("PEA_Schema")
        }
        case "TDLinx" => {
          sb.append(CT.CurrentTime() + " : Inside Case : TDLinx\n")
          Raw_File = adl_path + Prop.getProperty("TDLinks_Raw_file")
          Stage_File = adl_path + Prop.getProperty("TDLinks_Stage_File")
          Schema = Prop.getProperty("TDLinks_Schema")
        }
        case "Walmart_Icecream_POS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Icecream_POS\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Stage_File")
          Schema = Prop.getProperty("Walmart_ICECREAM_POS_Schema")
        }
        case "Walmart_Hairoil_POS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Hairoil_POS\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_POS_HAIROIL_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Walmart_POS_HAIROIL_Stage_File")
          Schema = Prop.getProperty("Walmart_ULHairOil_POS_Schema")
        }
        case "Walmart_Tea_POS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Tea_POS\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_POS_TEA_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Walmart_POS_TEA_Stage_File")
          Schema = Prop.getProperty("Walmart_TEA_POS_Schema")
        }
        case "Walmart_Spread_POS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_Spread_POS\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Stage_File")
          Schema = Prop.getProperty("Walmart_SPREADS_POS_Schema")
        }
        case "Walmart_SASC_POS" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Walmart_SASC_POS\n")
          Raw_File = adl_path + Prop.getProperty("Walmart_POS_SASC_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Walmart_POS_SASC_Stage_File")
          Schema = Prop.getProperty("Walmart_SASC_POS_Schema")
        }
        case "Nielsen_PPG" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_PPG\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_PPG_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_PPG_Stage_File")
          Schema = Prop.getProperty("Nielsen_PPG_Schema")
        }
        case "Nielsen_DHC_Market" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_DHC_Market\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_DHC_Market_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_DHC_Market_Stage_File")
          Schema = Prop.getProperty("NielsenDHCMarket_Schema")
        }
        case "Nielsen_Period" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_Period\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_Period_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_Period_Stage_File")
          Schema = Prop.getProperty("NielsenPeriod_Schema")
        }
        case "Nielsen_Product" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_Product\n")
          Raw_File = adl_path + Prop.getProperty("Nielsen_Product_Raw_File")
          Stage_File = adl_path + Prop.getProperty("Nielsen_Product_Stage_File")
          Schema = Prop.getProperty("NielsenProduct_Schema")
        }
        case "Nielsen_Demo" => {
          sb.append(CT.CurrentTime() + " : Inside Case : Nielsen_Demo\n")
          Raw_File = adl_path + Prop.getProperty("Demographic_Raw_file")
          Stage_File = adl_path + Prop.getProperty("Demographic_Stage_File")
          Schema = Prop.getProperty("Demographic_Schema")
        }
        case other => {
          sb.append(CT.CurrentTime() + " : The case did not matched\n")
          valid = 0
        }

      }
      val removeDoubleQuotes = udf[String, String]((d: String) =>
        if (d != null) d.replaceAll("\"", "").toString
        else "")
      if (valid == 1) {
        Error_Folder_Path = Raw_File.replaceAll("/Raw", "").replaceAll(adl_path, "").replaceAll(".csv", "").replaceAll("\\*", "")

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(Stage_File))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(Stage_File))
        }
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Raw_File.replaceAll(adl_path, "")))
        if (!listStatus.isEmpty) {
          for (i <- 0 to (listStatus.length - 1)) {
            StartTime = CT.CurrentTime()
            val Raw_File_Ind = listStatus.apply(i).getPath.toString()
            val FileName = Raw_File_Ind.split("/").last.replaceAll(".txt", ".csv")
            sb.append(CT.CurrentTime() + " : Processing file : " + FileName + "\n")

            sb.append(CT.CurrentTime() + " : Starting the validation process\n")
            val validate: Validation = new Validation()
            val Raw_DF = validate.CreateDataFrame(Raw_File_Ind, sqlContext, Delimited)
            RowCount = Raw_DF.count().toString()

            val Raw_DF_Columns = Raw_DF.columns.map(x => x.trim())

            val Master_Schema = Schema.replaceAll(" ", "_").split(Delimited)

            val Raw_DF_Columns_Clean = Raw_DF_Columns.mkString(",").replaceAll(" ", "_").split(",")

            /**
             * The following lines will do the validation whether the master file and datafile header match
             * if matching then return true else false
             */
            val validFlag = validate.ValidateSchema(Master_Schema, Raw_DF_Columns_Clean)

            if (validFlag) {
              sb.append(CT.CurrentTime() + " : Validation passed\n")
            } else {
              sb.append(CT.CurrentTime() + " : Validation failed\n")
              sb.append(CT.CurrentTime() + " : Expected was : " + Schema + "\n")
              sb.append("Actually is : " + Raw_DF_Columns.mkString(",") + "\n")
            }
            import sqlContext.implicits._

            /**
             * The following lines is used to check for extra comma or blank row
             */
            var Flag = false
            if (validFlag) {
              sb.append(CT.CurrentTime() + " : Writing file to Stage\n")
              del.Delete_File(hdfs, Stage_File.replaceAll(adl_path, "") + "/part*.csv")
              val fields = Master_Schema.map(fieldName => StructField(fieldName, StringType, nullable = true))
              val schema = StructType(fields)
              val Res_DF = sqlContext.createDataFrame(Raw_DF.rdd, schema)

              val DF_Column = schema.fieldNames
              var Empty_Df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
              val DF_Column_length = DF_Column.length - 1
              //Empty_Df = Res_DF.na.fill(" ")
              Empty_Df = Res_DF
              for (i <- 0 to DF_Column_length) {
                val colName = DF_Column.apply(i)
                Empty_Df = Empty_Df.withColumn(colName, removeDoubleQuotes(col(colName)))
              }
              /**
               * New Code end here
               */

              //Archive_Flag = archive.ArchiveFile(sqlContext, hdfs, hadoopConf, adl_path, Res_DF, Raw_File_Ind, Stage_File)
              Empty_Df.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Stage_File)

              val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Stage_File.replaceAll(adl_path, "") + "/part*.csv"))
              var path_04 = listStatusReName(0).getPath()
              Flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(Stage_File.replaceAll(adl_path, "") + "/" + FileName))
              if (Flag) {
                sb.append(CT.CurrentTime() + " : Writing file to Stage succesful and renamed\n")
              }
            }

            /**
             * Delete Raw File
             */

            val delete_raw_flag = del.Delete_File(hdfs, Raw_File_Ind)

            EndTime = CT.CurrentTime()

            val Raw_To_Stage_Error_Folder_Path = Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path
            if (validFlag) {
              if (Flag) {
                sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, the file is processed successfully\n")
                TablePush.ErrorHandle(FileName, "Processed", "NA", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
                //log.info(Raw_File_Ind+" Processed")

              } else {
                sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, the file is not processed successfully, some issue Renameing Failed / File already present in Stage\n")
                TablePush.ErrorHandle(FileName, "Rejected", "File Renameing Failed / File already present in Stage", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
                //log.info(Raw_File_Ind+" File Renameing Failed / File already present in Stage")

              }
            } else {
              sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, The schema validation has failed\n")
              TablePush.ErrorHandle(FileName, "Rejected", "Schema validation failed", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
              //log.info(Raw_File_Ind+" Schema validation failed")

            }

          }
        } else {
          EndTime = CT.CurrentTime()
          sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, No file found , Searched Raw file at : " + Raw_File + "\n")
          val TablePush: RawToStageError = new RawToStageError()
          TablePush.ErrorHandle(Raw_File, "Rejected", "File Not Found", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
          //log.info(Raw_File+" File Not Found")

        }
      } else if (valid == 2) {
        sb.append(CT.CurrentTime() + " : This is a TargetGRM file, it will be .txt file with no schema. We will only check for column length\n")
        Error_Folder_Path = Raw_File.replaceAll("/Raw", "").replaceAll(adl_path, "").replaceAll(".txt", "").replaceAll("\\*", "")

        if (hdfs.isDirectory(new org.apache.hadoop.fs.Path(Stage_File))) {
          del.Delete_File(hdfs, Stage_File)
        }
         hdfs.mkdirs(new org.apache.hadoop.fs.Path(Stage_File))

        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Raw_File.replaceAll(adl_path, "")))
        if (!listStatus.isEmpty) {
          for (i <- 0 to (listStatus.length - 1)) {
            var validFlag = false
            var Flag = false
            StartTime = CT.CurrentTime()
            val Raw_File_Ind = listStatus.apply(i).getPath.toString()
            val FileName = Raw_File_Ind.split("/").last.replaceAll(".txt", "")
            sb.append(CT.CurrentTime() + " : Processing file : " + FileName + "\n")

            val LineRdd = sc.textFile(Raw_File_Ind)
            val FirstLine = LineRdd.first()
            var Raw_DF_schema_length = 1

            for (c <- FirstLine) {
              if (c == '|') {
                Raw_DF_schema_length = Raw_DF_schema_length + 1
              }
            }

            sb.append(CT.CurrentTime() + " : Raw_DF_schema_length : " + Raw_DF_schema_length + "\n")

            val Master_Schema = Schema.replaceAll(" ", "_").split(",")

            val MasterDF_schema_length = Master_Schema.length

            sb.append(CT.CurrentTime() + " : MasterDF_schema_length : " + MasterDF_schema_length + "\n")
            if (Raw_DF_schema_length == MasterDF_schema_length) {

              val CleanRdd = LineRdd.map(_.replaceAll("\"", ""))
              RowCount = CleanRdd.count().toString()
              CleanRdd.saveAsTextFile(Stage_File + "/temp_" + FileName)
              Flag = hdfs.isFile(new org.apache.hadoop.fs.Path(Stage_File + "/temp_" + FileName + "/part-00000"))
              validFlag = true
            }
            /**
             * Delete Raw File
             */

            val delete_raw_flag = del.Delete_File(hdfs, Raw_File_Ind)

            EndTime = CT.CurrentTime()

            val Raw_To_Stage_Error_Folder_Path = Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path
            if (validFlag) {
              if (Flag) {
                sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, the file is processed successfully\n")
                TablePush.ErrorHandle(FileName, "Processed", "NA", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
                //log.info(Raw_File_Ind+" Processed")

              } else {
                sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, the file is not processed successfully, some issue Renameing Failed / File already present in Stage\n")
                TablePush.ErrorHandle(FileName, "Rejected", "File Renameing Failed / File already present in Stage", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
                //log.info(Raw_File_Ind+" File Renameing Failed / File already present in Stage")

              }
            } else {
              sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, The schema validation has failed\n")
              TablePush.ErrorHandle(FileName, "Rejected", "Schema validation failed", RowCount, StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)
              //log.info(Raw_File_Ind+" Schema validation failed")

            }
          }
        }
      }

    } catch {
      case t: Throwable =>
       {
          t.printStackTrace()

          EndTime = CT.CurrentTime()
          sb.append(CT.CurrentTime() + " : Writing Error file to Error folder, EXCEPTION occured while processing : " + t.getMessage() + "\n")
          val TablePush: RawToStageError = new RawToStageError()
          TablePush.ErrorHandle(Raw_File, "Exception Occured while Processing", "", "", StartTime, EndTime, Raw_to_Stage_ErrorFile + "/date/" + Error_Folder_Path, adl_path, sc, sqlContext, hdfs, hadoopConf)

        }

    } finally {
      logg.InsertLog(adl_path, FolderPath_temp, input_parameter, sb.toString())
      sc.stop()
    }

  }

}
