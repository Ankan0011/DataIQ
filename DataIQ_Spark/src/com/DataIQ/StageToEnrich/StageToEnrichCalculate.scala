/**
 * Stage To Enrich Calculation
 */
package com.DataIQ.StageToEnrich

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.DataIQ.Resource.Property
import com.DataIQ.StageToEnrichProcess.WalmartItem
import com.DataIQ.StageToEnrichProcess.ProcessCalculate
import com.DataIQ.StageToEnrichProcess.PriceList
import com.DataIQ.StageToEnrichProcess.StoreToRegion
import com.DataIQ.StageToEnrichProcess.UPCToPPG
import com.DataIQ.StageToEnrichProcess.ProcessCalculate
import com.DataIQ.StageToEnrichProcess.TargetPOS_Master
import com.DataIQ.StageToEnrichProcess.TargetCDTcalculate
import com.DataIQ.StageToEnrichProcess.NielsenCustomDHCMarket
import com.DataIQ.StageToEnrichProcess.NielsenPeriod
import com.DataIQ.StageToEnrichProcess.NielsenProduct
import com.DataIQ.StageToEnrichProcess.WalmartStoreUL
import com.DataIQ.StageToEnrichProcess.PEA
import com.DataIQ.StageToEnrichProcess.TDLinx
import com.DataIQ.StageToEnrichProcess.NielsenDemographic
import com.DataIQ.StageToEnrichProcess.MissingWeekData
import com.DataIQ.StageToEnrichProcess.AssortmentCalculation
import com.DataIQ.Resource.LogWrite
import com.DataIQ.StageToEnrichProcessCalculate.NielsenScanTrack
import com.DataIQ.StageToEnrichProcessCalculate.TargetPOS_SALES
import com.DataIQ.StageToEnrichProcessCalculate.TargetPOS_INV

import com.DataIQ.StageToEnrichProcessCalculate.Nielsen_PPG
import com.DataIQ.StageToEnrichProcessCalculate.WalmartPOS_All
import com.DataIQ.StageToEnrichProcessCalculate.WalmartPOS_USASCC
import com.DataIQ.StageToEnrichProcessCalculate.TargetPOS_GRM

object StageToEnrichCalculate {
  def main(args: Array[String]): Unit = {

    var input_parameter = ""
    var Second_param = ""
    var adl_path = ""

    val Prop: Property = new Property()

    val spark_serializer = Prop.getProperty("spark_serializer")
    val spark_executor_cores = Prop.getProperty("spark_executor_cores")
    val spark_executor_instances = Prop.getProperty("spark_executor_instances")
    val spark_yarn_containerLauncherMaxThreads = Prop.getProperty("spark_yarn_containerLauncherMaxThreads")

    val Sparkconf = new SparkConf().setAppName("StageToEnrichCalculate")
    Sparkconf.set("spark.serializer", spark_serializer)
    Sparkconf.set("spark.executor.cores", spark_executor_cores).set("spark.executor.instances", spark_executor_instances).set("spark.yarn.containerLauncherMaxThreads", spark_yarn_containerLauncherMaxThreads)

    val sc = new SparkContext(Sparkconf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    //val logg: LogWrite = new LogWrite()

    try {
      input_parameter = args(0)
      Second_param = args(1)
      adl_path = args(2)

      val Stage_To_Enrich_Error = adl_path + Prop.getProperty("Stage_to_Enrich_ErrorRecord")
      //val adl_path = Prop.getProperty("adl_path")
      val HiveDatabase = Prop.getProperty("HiveDatabase")
      val FolderPath_temp = adl_path + Prop.getProperty("Stage_To_Enriche_LogInfo")
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf)

      input_parameter match {

        case "Nielsen_Scan_UPC" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Scan_UPC", "inside case: Nielsen_Scan_UPC")
          val Nielsen_Scan_UPC_EnrichFile = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Enrich_File")
          val Nielsen_Scan_UPC_StageFile = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Stage_File")
          val Nielsen_Scan_UPC_Schema = Prop.getProperty("Nielsen_Scan_UPC_File_Schema")
          val Nielsen_Scan_UPC_Product = adl_path + Prop.getProperty("Nielsen_Product_Master_Enrich_File")
          val Nielsen_Scan_UPC_Period = adl_path + Prop.getProperty("Nielsen_Period_Master_Enrich_File")
          val Nielsen_Scan_UPC_DHCMarket = adl_path + Prop.getProperty("Nielsen_DHCMarket_Master_Enrich_File")
          val Nielsen_Scan_UPC_EnrichFileName = Prop.getProperty("Nielsen_Scan_UPC_Enrich_File_Name")
          //val Nielsen_Scan_UPC_IncrementFolder = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Increment_Folder") + "/" + Second_param
          //val Nielsen_Scan_UPC_RestatementFolder = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Restatement_Folder") + "/" + Second_param
          val Nielsen_Scan_UPC_UpdatedFilesFolder = adl_path + Prop.getProperty("Nielsen_Scan_UPC_UpdatedFiles_Folder") + "/" + Second_param
          val Nielsen_Scan_UPC_FieldName = Prop.getProperty("Nielsen_Scan_UPC_Field_Name")
          val Nielsen_Scan_UPC_IncrementFileName = Prop.getProperty("Nielsen_Scan_UPC_Increment_File_Name")
          val Nielsen_Scan_UPC_RestatementFileName = Prop.getProperty("Nielsen_Scan_UPC_Restatement_File_Name")
          val Nielsen_Scan_UPC_TableName = Prop.getProperty("Nielsen_Scan_UPC_Table_Name")
          val Nielsen_Scan_UPC_TablePath = adl_path + Prop.getProperty("Nielsen_Scan_UPC_Table_Path")
          val NielsenScan: NielsenScanTrack = new NielsenScanTrack()
          //NielsenScan.IncrementNielsenCustomScanUPC(Nielsen_Scan_UPC_StageFile, Nielsen_Scan_UPC_EnrichFile, Nielsen_Scan_UPC_Product, Nielsen_Scan_UPC_Period, Nielsen_Scan_UPC_DHCMarket, Nielsen_Scan_UPC_Schema, Nielsen_Scan_UPC_EnrichFileName, Nielsen_Scan_UPC_IncrementFolder, Nielsen_Scan_UPC_RestatementFolder, Nielsen_Scan_UPC_FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          NielsenScan.IncrementNielsenCustomScanUPC(Nielsen_Scan_UPC_StageFile, Nielsen_Scan_UPC_EnrichFile, Nielsen_Scan_UPC_Product, Nielsen_Scan_UPC_Period, Nielsen_Scan_UPC_DHCMarket, Nielsen_Scan_UPC_Schema, Nielsen_Scan_UPC_EnrichFileName, Nielsen_Scan_UPC_UpdatedFilesFolder, Nielsen_Scan_UPC_FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }

        case "Walmart_ICECREAM_ITEM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_ICECREAM_ITEM", "inside case: Walmart_ICECREAM_ITEM")
          val Walmart_ICECREAM_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Enrich_File")
          val Walmart_ICECREAM_ITEM_StageFile = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Stage_File")
          val Walmart_ICECREAM_ITEM_FileSchema = Prop.getProperty("Walmart_ICECREAM_ITEM_File_Schema")
          val Walmart_ICECREAM_ITEM_EnrichFileName = Prop.getProperty("Walmart_ICECREAM_ITEM_Enrich_File_Name")
          val Walmart_ICECREAM_ITEM_IncrementFileName = Prop.getProperty("Walmart_ICECREAM_ITEM_Increment_File_Name")
          val Walmart_ICECREAM_ITEM_IncrementFolder = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Increment_Folder") + "/" + Second_param
          val WalmartICECREAMITEM: WalmartItem = new WalmartItem()
          WalmartICECREAMITEM.IncrementWalmartItem(Walmart_ICECREAM_ITEM_EnrichFile, Walmart_ICECREAM_ITEM_StageFile, Walmart_ICECREAM_ITEM_FileSchema, Walmart_ICECREAM_ITEM_EnrichFileName, Walmart_ICECREAM_ITEM_IncrementFileName, Walmart_ICECREAM_ITEM_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)

        }
        case "Walmart_ULHairOil_ITEM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_ULHairOil_ITEM", "inside case: Walmart_ULHairOil_ITEM")
          val Walmart_ULHairOil_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Enrich_File")
          val Walmart_ULHairOil_ITEM_StageFile = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Stage_File")
          val Walmart_ULHairOil_ITEM_FileSchema = Prop.getProperty("Walmart_ULHairOil_ITEM_File_Schema")
          val Walmart_ULHairOil_ITEM_EnrichFileName = Prop.getProperty("Walmart_ULHairOil_ITEM_Enrich_File_Name")
          val Walmart_ULHairOil_ITEM_IncrementFileName = Prop.getProperty("Walmart_ULHairOil_ITEM_Increment_File_Name")
          val Walmart_ULHairOil_ITEM_IncrementFolder = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Increment_Folder") + "/" + Second_param
          val WalmartULHairOilITEM: WalmartItem = new WalmartItem()
          WalmartULHairOilITEM.IncrementWalmartItem(Walmart_ULHairOil_ITEM_EnrichFile, Walmart_ULHairOil_ITEM_StageFile, Walmart_ULHairOil_ITEM_FileSchema, Walmart_ULHairOil_ITEM_EnrichFileName, Walmart_ULHairOil_ITEM_IncrementFileName, Walmart_ULHairOil_ITEM_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Walmart_TEA_ITEM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_TEA_ITEM", "inside case: Walmart_TEA_ITEM")
          val Walmart_TEA_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_TEA_ITEM_Enrich_File")
          val Walmart_TEA_ITEM_StageFile = adl_path + Prop.getProperty("Walmart_TEA_ITEM_Stage_File")
          val Walmart_TEA_ITEM_FileSchema = Prop.getProperty("Walmart_TEA_ITEM_File_Schema")
          val Walmart_TEA_ITEM_ITEM_EnrichFileName = Prop.getProperty("Walmart_TEA_ITEM_ITEM_Enrich_File_Name")
          val Walmart_TEA_ITEM_ITEM_IncrementFileName = Prop.getProperty("Walmart_TEA_ITEM_ITEM_Increment_File_Name")
          val Walmart_TEA_ITEM_ITEM_IncrementFolder = adl_path + Prop.getProperty("Walmart_TEA_ITEM_ITEM_Increment_Folder") + "/" + Second_param
          val WalmartTEAITEM: WalmartItem = new WalmartItem()
          WalmartTEAITEM.IncrementWalmartItem(Walmart_TEA_ITEM_EnrichFile, Walmart_TEA_ITEM_StageFile, Walmart_TEA_ITEM_FileSchema, Walmart_TEA_ITEM_ITEM_EnrichFileName, Walmart_TEA_ITEM_ITEM_IncrementFileName, Walmart_TEA_ITEM_ITEM_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Walmart_SPREADS_ITEM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_SPREADS_ITEM", "inside case: Walmart_SPREADS_ITEM")
          val Walmart_SPREADS_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Enrich_File")
          val Walmart_SPREADS_ITEM_StageFile = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Stage_File")
          val Walmart_SPREADS_ITEM_FileSchema = Prop.getProperty("Walmart_SPREADS_ITEM_File_Schema")
          val Walmart_SPREADS_ITEM_EnrichFileName = Prop.getProperty("Walmart_SPREADS_ITEM_Enrich_File_Name")
          val Walmart_SPREADS_ITEM_IncrementFileName = Prop.getProperty("Walmart_SPREADS_ITEM_Increment_File_Name")
          val Walmart_SPREADS_ITEM_IncrementFolder = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Increment_Folder") + "/" + Second_param
          val WalmartSPREADSITEM: WalmartItem = new WalmartItem()
          WalmartSPREADSITEM.IncrementWalmartItem(Walmart_SPREADS_ITEM_EnrichFile, Walmart_SPREADS_ITEM_StageFile, Walmart_SPREADS_ITEM_FileSchema, Walmart_SPREADS_ITEM_EnrichFileName, Walmart_SPREADS_ITEM_IncrementFileName, Walmart_SPREADS_ITEM_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Walmart_USASCC_ITEM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_USASCC_ITEM", "inside case: Walmart_USASCC_ITEM")
          val Walmart_USASCC_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Enrich_File")
          val Walmart_USASCC_ITEM_StageFile = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Stage_File")
          val Walmart_USASCC_ITEM_FileSchema = Prop.getProperty("Walmart_USASCC_ITEM_File_Schema")
          val Walmart_USASCC_ITEM_EnrichFileName = Prop.getProperty("Walmart_USASCC_ITEM_Enrich_File_Name")
          val Walmart_USASCC_ITEM_IncrementFileName = Prop.getProperty("Walmart_USASCC_ITEM_Increment_File_Name")
          val Walmart_USASCC_ITEM_IncrementFolder = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Increment_Folder") + "/" + Second_param
          val WalmartUSASCCITEM: WalmartItem = new WalmartItem()
          WalmartUSASCCITEM.IncrementWalmartItem(Walmart_USASCC_ITEM_EnrichFile, Walmart_USASCC_ITEM_StageFile, Walmart_USASCC_ITEM_FileSchema, Walmart_USASCC_ITEM_EnrichFileName, Walmart_USASCC_ITEM_IncrementFileName, Walmart_USASCC_ITEM_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "8451_SHCD" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "8451_SHCD", "inside case: 8451_SHCD")
          val SHCD_Enrich_File = adl_path + Prop.getProperty("8451_SHCD_Enrich_File")
          val SHCD_Stage_File = adl_path + Prop.getProperty("8451_SHCD_Stage_File")
          val SHCD_File_Schema = Prop.getProperty("8451_SHCD_File_Schema")
          val SHCD_Enrich_File_Name = Prop.getProperty("8451_SHCD_Enrich_File_Name")
          val SHCD_Increment_File_Name = Prop.getProperty("8451_SHCD_Increment_File_Name")
          val SHCD_Increment_Folder = adl_path + Prop.getProperty("8451_SHCD_Increment_Folder") + "/" + Second_param
          val SHCD: ProcessCalculate = new ProcessCalculate()
          SHCD.IncrementalFeature(SHCD_Enrich_File, SHCD_Stage_File, SHCD_File_Schema, SHCD_Enrich_File_Name, SHCD_Increment_File_Name, SHCD_Increment_Folder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "8451_Styling" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "8451_Styling", "inside case: 8451_Styling")
          val Styling_Enrich_File = adl_path + Prop.getProperty("8451_Styling_Enrich_File")
          val Styling_Stage_File = adl_path + Prop.getProperty("8451_Styling_Stage_File")
          val Styling_File_Schema = Prop.getProperty("8451_Styling_File_Schema")
          val Styling_Enrich_File_Name = Prop.getProperty("8451_Styling_Enrich_File_Name")
          val Styling_Increment_File_Name = Prop.getProperty("8451_Styling_Increment_File_Name")
          val Styling_Increment_Folder = adl_path + Prop.getProperty("8451_Styling_Increment_Folder") + "/" + Second_param
          val Styling: ProcessCalculate = new ProcessCalculate()
          Styling.IncrementalFeature(Styling_Enrich_File, Styling_Stage_File, Styling_File_Schema, Styling_Enrich_File_Name, Styling_Enrich_File_Name, Styling_Increment_Folder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "InfoScout_Trip" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "InfoScout_Trip", "inside case: InfoScout_Trip")
          val InfoScout_Trip_EnrichFile = adl_path + Prop.getProperty("InfoScout_Trip_Enrich_File")
          val InfoScout_Trip_StageFile = adl_path + Prop.getProperty("InfoScout_Trip_Stage_File")
          val InfoScout_Trip_FileSchema = Prop.getProperty("InfoScout_Trip_File_Schema")
          val InfoScout_Trip_EnrichFileName = Prop.getProperty("InfoScout_Trip_Enrich_File_Name")
          val InfoScout_Trip_IncrementFileName = Prop.getProperty("InfoScout_Trip_Increment_File_Name")
          val InfoScout_Trip_IncrementFolder = adl_path + Prop.getProperty("InfoScout_Trip_Increment_Folder") + "/" + Second_param
          val InfoScoutTrip: ProcessCalculate = new ProcessCalculate()
          InfoScoutTrip.IncrementalFeature(InfoScout_Trip_EnrichFile, InfoScout_Trip_StageFile, InfoScout_Trip_FileSchema, InfoScout_Trip_EnrichFileName, InfoScout_Trip_IncrementFileName, InfoScout_Trip_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "InfoScout_Feed" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "InfoScout_Feed", "inside case: InfoScout_Feed")
          val InfoScout_Feed_EnrichFile = adl_path + Prop.getProperty("InfoScout_Feed_Enrich_File")
          val InfoScout_Feed_StageFile = adl_path + Prop.getProperty("InfoScout_Feed_Stage_File")
          val InfoScout_Feed_FileSchema = Prop.getProperty("InfoScout_Feed_File_Schema")
          val InfoScout_Feed_EnrichFileName = Prop.getProperty("InfoScout_Feed_Enrich_File_Name")
          val InfoScout_Feed_IncrementFileName = Prop.getProperty("InfoScout_Feed_Increment_File_Name")
          val InfoScout_Feed_IncrementFolder = adl_path + Prop.getProperty("InfoScout_Feed_Increment_Folder") + "/" + Second_param
          val InfoScoutFeed: ProcessCalculate = new ProcessCalculate()
          InfoScoutFeed.IncrementalFeature(InfoScout_Feed_EnrichFile, InfoScout_Feed_StageFile, InfoScout_Feed_FileSchema, InfoScout_Feed_EnrichFileName, InfoScout_Feed_IncrementFileName, InfoScout_Feed_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "PriceList" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "PriceList", "inside case: PriceList")
          val PriceList_Enrich_File = adl_path + Prop.getProperty("PriceList_Enrich_File")
          val PriceList_Stage_File = adl_path + Prop.getProperty("PriceList_Stage_File")
          val PriceList_File_Schema = Prop.getProperty("PriceList_File_Schema")
          val PriceList_Enrich_File_Name = Prop.getProperty("PriceList_Enrich_File_Name")
          val PriceList_Increment_File_Name = Prop.getProperty("PriceList_Increment_File_Name")
          val PriceList_Increment_Folder = adl_path + Prop.getProperty("PriceList_Increment_Folder") + "/" + Second_param
          val pricelist: PriceList = new PriceList()
          pricelist.IncrementalPricelist(PriceList_Enrich_File, PriceList_Stage_File, PriceList_File_Schema, PriceList_Enrich_File_Name, PriceList_Increment_File_Name, PriceList_Increment_Folder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "WalmartCalendar" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WalmartCalendar", "inside case: WalmartCalendar")
          val WalmartCalendar_EnrichFile = adl_path + Prop.getProperty("WalmartCalendar_Enrich_File")
          val WalmartCalendar_StageFile = adl_path + Prop.getProperty("WalmartCalendar_Stage_File")
          val WalmartCalendar_FileSchema = Prop.getProperty("WalmartCalendar_File_Schema")
          val WalmartCalendar_EnrichFileName = Prop.getProperty("WalmartCalendar_Enrich_File_Name")
          val WalmartCalendar_IncrementFileName = Prop.getProperty("WalmartCalendar_Increment_File_Name")
          val WalmartCalendar_IncrementFolder = adl_path + Prop.getProperty("WalmartCalendar_Increment_Folder") + "/" + Second_param
          val WalmartCalendar: ProcessCalculate = new ProcessCalculate()
          WalmartCalendar.IncrementalFeature(WalmartCalendar_EnrichFile, WalmartCalendar_StageFile, WalmartCalendar_FileSchema, WalmartCalendar_EnrichFileName, WalmartCalendar_IncrementFileName, WalmartCalendar_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "StoreRegionMapping" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "StoreRegionMapping", "inside case: StoreRegionMapping")
          val StoreRegionMapping_EnrichFile = adl_path + Prop.getProperty("StoreRegionMapping_Enrich_File")
          val StoreRegionMapping_StageFile = adl_path + Prop.getProperty("StoreRegionMapping_Stage_File")
          val StoreRegionMapping_FileSchema = Prop.getProperty("StoreRegionMapping_File_Schema")
          val StoreRegionMapping_EnrichFileName = Prop.getProperty("StoreRegionMapping_Enrich_File_Name")
          val StoreRegionMapping_IncrementFileName = Prop.getProperty("StoreRegionMapping_Increment_File_Name")
          val StoreRegionMapping_IncrementFolder = adl_path + Prop.getProperty("StoreRegionMapping_Increment_Folder") + "/" + Second_param
          val TDLinks_MasterEnrichFile = adl_path + Prop.getProperty("TDLinks_Master_Enrich_File")
          val StoreRegionMapping: StoreToRegion = new StoreToRegion()
          StoreRegionMapping.IncrementalStore2Region(StoreRegionMapping_EnrichFile, StoreRegionMapping_StageFile, StoreRegionMapping_FileSchema, StoreRegionMapping_EnrichFileName, StoreRegionMapping_IncrementFileName, StoreRegionMapping_IncrementFolder, TDLinks_MasterEnrichFile, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "NielsenUPCtoPPGmapping" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "NielsenUPCtoPPGmapping", "inside case: NielsenUPCtoPPGmapping")
          val NielsenUPCtoPPGmapping_EnrichFile = adl_path + Prop.getProperty("NielsenUPCtoPPGmapping_Enrich_File")
          val NielsenUPCtoPPGmapping_StageFile = adl_path + Prop.getProperty("NielsenUPCtoPPGmapping_Stage_File")
          val NielsenUPCtoPPGmapping_FileSchema = Prop.getProperty("NielsenUPCtoPPGmapping_File_Schema")
          val NielsenUPCtoPPGmapping_EnrichFileName = Prop.getProperty("NielsenUPCtoPPGmapping_Enrich_File_Name")
          val NielsenUPCtoPPGmapping_IncrementFileName = Prop.getProperty("NielsenUPCtoPPGmapping_Increment_File_Name")
          val NielsenUPCtoPPGmapping_IncrementFolder = adl_path + Prop.getProperty("NielsenUPCtoPPGmapping_Increment_Folder") + "/" + Second_param
          val NielsenProduct_MasterEnrichFile = adl_path + Prop.getProperty("NielsenProduct_Master_EnrichFile")
          val NielsenUPCtoPPGmapping: UPCToPPG = new UPCToPPG()
          NielsenUPCtoPPGmapping.IncrementalUPC2PPG(NielsenUPCtoPPGmapping_EnrichFile, NielsenUPCtoPPGmapping_StageFile, NielsenUPCtoPPGmapping_FileSchema, NielsenUPCtoPPGmapping_EnrichFileName, NielsenUPCtoPPGmapping_IncrementFileName, NielsenUPCtoPPGmapping_IncrementFolder, NielsenProduct_MasterEnrichFile, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "PeriodMappingGWTNCalendar" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "PeriodMappingGWTNCalendar", "inside case: PeriodMappingGWTNCalendar")
          val PeriodMappingGWTNCalendar_EnrichFile = adl_path + Prop.getProperty("PeriodMappingGWTNCalendar_Enrich_File")
          val PeriodMappingGWTNCalendar_StageFile = adl_path + Prop.getProperty("PeriodMappingGWTNCalendar_Stage_File")
          val PeriodMappingGWTNCalendar_FileSchema = Prop.getProperty("PeriodMappingGWTNCalendar_File_Schema")
          val PeriodMappingGWTNCalendar_EnrichFileName = Prop.getProperty("PeriodMappingGWTNCalendar_Enrich_File_Name")
          val PeriodMappingGWTNCalendar_IncrementFileName = Prop.getProperty("PeriodMappingGWTNCalendar_Increment_File_Name")
          val PeriodMappingGWTNCalendar_IncrementFolder = adl_path + Prop.getProperty("PeriodMappingGWTNCalendar_Increment_Folder") + "/" + Second_param
          val PeriodMappingGWTNCalendar: ProcessCalculate = new ProcessCalculate()
          PeriodMappingGWTNCalendar.IncrementalFeature(PeriodMappingGWTNCalendar_EnrichFile, PeriodMappingGWTNCalendar_StageFile, PeriodMappingGWTNCalendar_FileSchema, PeriodMappingGWTNCalendar_EnrichFileName, PeriodMappingGWTNCalendar_IncrementFileName, PeriodMappingGWTNCalendar_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Holiday" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Holiday", "inside case: Holiday")
          val Holiday_EnrichFile = adl_path + Prop.getProperty("Holiday_Enrich_File")
          val Holiday_StageFile = adl_path + Prop.getProperty("Holiday_Stage_File")
          val Holiday_FileSchema = Prop.getProperty("Holiday_File_Schema")
          val Holiday_EnrichFileName = Prop.getProperty("Holiday_Enrich_File_Name")
          val Holiday_IncrementFileName = Prop.getProperty("Holiday_Increment_File_Name")
          val Holiday_IncrementFolder = adl_path + Prop.getProperty("Holiday_Increment_Folder") + "/" + Second_param
          val Holiday: ProcessCalculate = new ProcessCalculate()
          Holiday.IncrementalFeature(Holiday_EnrichFile, Holiday_StageFile, Holiday_FileSchema, Holiday_EnrichFileName, Holiday_IncrementFileName, Holiday_IncrementFolder, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "TDLinx_Mapping" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TDLinx_Mapping", "inside case: TDLinx_Mapping")
          val TDLinx_Mapping_EnrichFile = adl_path + Prop.getProperty("TDLinx_Mapping_Enrich_File")
          val TDLinx_Mapping_StageFile = adl_path + Prop.getProperty("TDLinx_Mapping_Stage_File")
          val TDLinx_Mapping_FileSchema = Prop.getProperty("TDLinx_Mapping_File_Schema")
          val TDLinx_Mapping_Enrich_FileName = Prop.getProperty("TDLinx_Mapping_Enrich_File_Name")
          val TDLinx_Mapping_Increment_FileName = Prop.getProperty("TDLinx_Mapping_Increment_File_Name")
          val TDLinx_Mapping_IncrementFolder = adl_path + Prop.getProperty("TDLinx_Mapping_Increment_Folder") + "/" + Second_param
          val TDLinks_MasterEnrichFile = adl_path + Prop.getProperty("TDLinks_Master_Enrich_File")
          val TDLinxMap: StoreToRegion = new StoreToRegion()
          TDLinxMap.IncrementalStore2Region(TDLinx_Mapping_EnrichFile, TDLinx_Mapping_StageFile, TDLinx_Mapping_FileSchema, TDLinx_Mapping_Enrich_FileName, TDLinx_Mapping_Increment_FileName, TDLinx_Mapping_IncrementFolder, TDLinks_MasterEnrichFile, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Nielsen_Elasticity" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Elasticity", "inside case: Nielsen_Elasticity")
          val Enrich_File = adl_path + Prop.getProperty("Nielsen_Elasticity_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Nielsen_Elasticity_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Nielsen_Elasticity")
          val schemaString = Prop.getProperty("Nielsen_Elasticity_Schema")
          val Nielsen_Elasticity = Prop.getProperty("Nielsen_Elasticity")
          val Nielsen_Elasticity_Incremental_Folder_Path = adl_path + Prop.getProperty("Nielsen_Elasticity_Incremental_Folder_Path") + "/" + Second_param
          val Nielsen_Elasticity_key_Field = Prop.getProperty("Nielsen_Elasticity_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetMaster(Stage_File, Enrich_File, Nielsen_Elasticity, Nielsen_Elasticity_Incremental_Folder_Path, Nielsen_Elasticity, Nielsen_Elasticity_key_Field, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, FolderPath_temp)
        }
        case "Weather" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Weather", "inside case: Weather")
          val Enrich_File = adl_path + Prop.getProperty("Weather_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Weather_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Weather")
          val schemaString = Prop.getProperty("Weather_Schema")
          val Weather_Incremental_Folder_Path = adl_path + Prop.getProperty("Weather_Incremental_Folder_Path") + "/" + Second_param
          val Weather_key_Field = Prop.getProperty("Weather_key_Field")
          val weather: ProcessCalculate = new ProcessCalculate()
          weather.IncrementWeather(Stage_File, Enrich_File, Enrich_File_Name, Weather_Incremental_Folder_Path, Enrich_File_Name, Weather_key_Field, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, FolderPath_temp)         
          }
        case "Target_GRM_LOCATION" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Target_GRM_LOCATION", "inside case: Target_GRM_LOCATION")
          val Target_GRM_LOCATION_EnrichFile = adl_path + Prop.getProperty("Target_GRM_LOCATION_EnrichFile")
          val Target_GRM_LOCATION_Stage_File = adl_path + Prop.getProperty("Target_GRM_LOCATION_Stage_File")
          val schemaString = Prop.getProperty("Target_GRM_LOCATION_Schema")
          val Enrich_File_Name = Prop.getProperty("Target_GRM_LOCATION")
          val Target_GRM_LOCATION_Incremental_Folder_Path = adl_path + Prop.getProperty("Target_GRM_LOCATION_Incremental_Folder_Path") + "/" + Second_param
          val Target_GRM_LOCATION_key_Field = Prop.getProperty("Target_GRM_LOCATION_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetMaster(Target_GRM_LOCATION_Stage_File, Target_GRM_LOCATION_EnrichFile, Enrich_File_Name, Target_GRM_LOCATION_Incremental_Folder_Path, Enrich_File_Name, Target_GRM_LOCATION_key_Field, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, FolderPath_temp)
        }
        case "Target_GRM_PRODUCT" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Target_GRM_PRODUCT", "inside case: Target_GRM_PRODUCT")
          val Target_GRM_PRODUCT_EnrichFile = adl_path + Prop.getProperty("Target_GRM_PRODUCT_EnrichFile")
          val Target_GRM_PRODUCT_Stage_File = adl_path + Prop.getProperty("Target_GRM_PRODUCT_Stage_File")
          val schemaString = Prop.getProperty("Target_GRM_PRODUCT_Schema")
          val Enrich_File_Name = Prop.getProperty("Target_GRM_PRODUCT")
          val Target_GRM_PRODUCT_Incremental_Folder_Path = adl_path + Prop.getProperty("Target_GRM_PRODUCT_Incremental_Folder_Path") + "/" + Second_param
          val Target_GRM_PRODUCT_key_Field = Prop.getProperty("Target_GRM_PRODUCT_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetMaster(Target_GRM_PRODUCT_Stage_File, Target_GRM_PRODUCT_EnrichFile, Enrich_File_Name, Target_GRM_PRODUCT_Incremental_Folder_Path, Enrich_File_Name, Target_GRM_PRODUCT_key_Field, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, FolderPath_temp)
        }
        case "TargetPOS_Location" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Location", "inside case: TargetPOS_Location")
          val Target_Location_Master_EnrichFile = adl_path + Prop.getProperty("Target_Location_Master_EnrichFile")
          val Target_Location_Master_Stage_File = adl_path + Prop.getProperty("Target_Location_Master_Stage_File")
          val Target_Location_Master_Schema = Prop.getProperty("Target_Location_Master_Schema")
          val Target_Location_Master = Prop.getProperty("Target_Location_Master")
          val TargetPOS_Location_Incremental_Folder_Path = adl_path + Prop.getProperty("TargetPOS_Location_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Location_key_Field = Prop.getProperty("TargetPOS_Location_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetMaster(Target_Location_Master_Stage_File, Target_Location_Master_EnrichFile, Target_Location_Master, TargetPOS_Location_Incremental_Folder_Path, Target_Location_Master, TargetPOS_Location_key_Field, sqlContext, sc, hadoopConf, hdfs, Target_Location_Master_Schema, adl_path, FolderPath_temp)
        }
        case "TargetPOS_Product" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Product", "inside case: TargetPOS_Product")

          val Target_Product_Master_EnrichFile = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Target_Product_Master_Stage_File = adl_path + Prop.getProperty("Target_Product_Master_Stage_File")
          val Target_Product_Master_Schema = Prop.getProperty("Target_Product_Master_Schema")
          val Target_Product_Master = Prop.getProperty("Target_Product_Master")
          val TargetPOS_Product_Incremental_Folder_Path = adl_path + Prop.getProperty("TargetPOS_Product_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Product_key_Field = Prop.getProperty("TargetPOS_Product_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetMaster(Target_Product_Master_Stage_File, Target_Product_Master_EnrichFile, Target_Product_Master, TargetPOS_Product_Incremental_Folder_Path, Target_Product_Master, TargetPOS_Product_key_Field, sqlContext, sc, hadoopConf, hdfs, Target_Product_Master_Schema, adl_path, FolderPath_temp)
        }
        
        case "STR_CNCPT" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Product", "inside case: TargetPOS_Product")

          val Target_Product_Master_EnrichFile = adl_path + Prop.getProperty("STR_CNCPT_EnrichFile")
          val Target_Product_Master_Stage_File = adl_path + Prop.getProperty("STR_CNCPT_Stage_File")
          val Target_Product_Master_Schema = Prop.getProperty("STR_CNCPT_Schema")
          val Target_Product_Master = Prop.getProperty("STR_CNCPT")
          val TargetPOS_Product_Incremental_Folder_Path = adl_path + Prop.getProperty("STR_CNCPT_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Product_key_Field1 = Prop.getProperty("STR_CNCPT_key_Field1")
          val TargetPOS_Product_key_Field2 = Prop.getProperty("STR_CNCPT_key_Field2")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetGRMMaster(Target_Product_Master_Stage_File, Target_Product_Master_EnrichFile, Target_Product_Master, TargetPOS_Product_Incremental_Folder_Path, Target_Product_Master, TargetPOS_Product_key_Field1,TargetPOS_Product_key_Field2, sqlContext, sc, hadoopConf, hdfs, Target_Product_Master_Schema, adl_path, FolderPath_temp)
        }
        case "GST_DEMOS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Product", "inside case: TargetPOS_Product")

          val Target_Product_Master_EnrichFile = adl_path + Prop.getProperty("GST_DEMOS_EnrichFile")
          val Target_Product_Master_Stage_File = adl_path + Prop.getProperty("GST_DEMOS_Stage_File")
          val Target_Product_Master_Schema = Prop.getProperty("GST_DEMOS_Schema")
          val Target_Product_Master = Prop.getProperty("GST_DEMOS")
          val TargetPOS_Product_Incremental_Folder_Path = adl_path + Prop.getProperty("GST_DEMOS_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Product_key_Field = Prop.getProperty("GST_DEMOS_key_Field")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetGSTDemo(Target_Product_Master_Stage_File, Target_Product_Master_EnrichFile, Target_Product_Master, TargetPOS_Product_Incremental_Folder_Path, Target_Product_Master, TargetPOS_Product_key_Field, sqlContext, sc, hadoopConf, hdfs, Target_Product_Master_Schema, adl_path, FolderPath_temp)
          
        }
        case "STR_CURR" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Product", "inside case: TargetPOS_Product")

          val Target_Product_Master_EnrichFile = adl_path + Prop.getProperty("STR_CURR_EnrichFile")
          val Target_Product_Master_Stage_File = adl_path + Prop.getProperty("STR_CURR_Stage_File")
          val Target_Product_Master_Schema = Prop.getProperty("STR_CURR_Schema")
          val Target_Product_Master = Prop.getProperty("STR_CURR")
          val TargetPOS_Product_Incremental_Folder_Path = adl_path + Prop.getProperty("STR_CURR_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Product_key_Field1 = Prop.getProperty("STR_CURR_key_Field1")
          val TargetPOS_Product_key_Field2 = Prop.getProperty("STR_CURR_key_Field2")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetGRMMaster(Target_Product_Master_Stage_File, Target_Product_Master_EnrichFile, Target_Product_Master, TargetPOS_Product_Incremental_Folder_Path, Target_Product_Master, TargetPOS_Product_key_Field1,TargetPOS_Product_key_Field2, sqlContext, sc, hadoopConf, hdfs, Target_Product_Master_Schema, adl_path, FolderPath_temp)
        }
        case "ITEM_HIER" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Product", "inside case: TargetPOS_Product")

          val Target_Product_Master_EnrichFile = adl_path + Prop.getProperty("ITEM_HIER_EnrichFile")
          val Target_Product_Master_Stage_File = adl_path + Prop.getProperty("ITEM_HIER_Stage_File")
          val Target_Product_Master_Schema = Prop.getProperty("ITEM_HIER_Schema")
          val Target_Product_Master = Prop.getProperty("ITEM_HIER")
          val TargetPOS_Product_Incremental_Folder_Path = adl_path + Prop.getProperty("ITEM_HIER_Incremental_Folder_Path") + "/" + Second_param
          val TargetPOS_Product_key_Field1 = Prop.getProperty("ITEM_HIER_key_Field1")
          val TargetPOS_Product_key_Field2 = Prop.getProperty("ITEM_HIER_key_Field2")
          val targetMaster: TargetPOS_Master = new TargetPOS_Master()
          targetMaster.IncrementTargetGRMMaster(Target_Product_Master_Stage_File, Target_Product_Master_EnrichFile, Target_Product_Master, TargetPOS_Product_Incremental_Folder_Path, Target_Product_Master, TargetPOS_Product_key_Field1,TargetPOS_Product_key_Field2, sqlContext, sc, hadoopConf, hdfs, Target_Product_Master_Schema, adl_path, FolderPath_temp)
        }

        case "TargetPOS_Sales" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Sales", "inside case: TargetPOS_Sales")
          val Enrich_File = adl_path + Prop.getProperty("Target_POS_Sales_EnrichFile")
          val TargetPOS_Sales = adl_path + Prop.getProperty("Target_POS_Sales_Stage_File")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val TargetPOS_Location = adl_path + Prop.getProperty("Target_Location_Master_EnrichFile")
          val Enrich_File_Name = Prop.getProperty("TargetPOS_Sales_Enrich_File_Name")
          val schemaString = Prop.getProperty("TargetPOS_Sales_schemaString")
          //val Target_POS_Sales_Increment_Folder = adl_path + Prop.getProperty("Target_POS_Sales_Increment_Folder") + "/" + Second_param
          val Field_Name = Prop.getProperty("Target_POS_Sales_Field_Name")
          //val Target_POS_Sales_Restatement_Folder = adl_path + Prop.getProperty("Target_POS_Sales_Restatement_Folder") + "/" + Second_param
          val Target_POS_Sales_UpdatedFiles_Folder = adl_path + Prop.getProperty("Target_POS_Sales_UpdatedFiles_Folder") + "/" + Second_param
          val Table_Name = Prop.getProperty("Target_POS_Sales_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Target_POS_Sales_Table_path")
          val IncrementLogic: TargetPOS_SALES = new TargetPOS_SALES()
          //IncrementLogic.IncrementalTarget(TargetPOS_Sales, Enrich_File, TargetPOS_Product, TargetPOS_Location, schemaString, Enrich_File_Name, Target_POS_Sales_Increment_Folder, Target_POS_Sales_Restatement_Folder, Field_Name, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          IncrementLogic.IncrementalTarget(TargetPOS_Sales, Enrich_File, TargetPOS_Product, TargetPOS_Location, schemaString, Enrich_File_Name, Target_POS_Sales_UpdatedFiles_Folder, Field_Name, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }

        case "TargetPOS_Inventory" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Inventory", "inside case: TargetPOS_Inventory")
          val Target_POS_inventory_EnrichFile = adl_path + Prop.getProperty("Target_POS_inventory_EnrichFile")
          val Target_POS_inventory_Stage_File = adl_path + Prop.getProperty("Target_POS_inventory_Stage_File")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val TargetPOS_Location = adl_path + Prop.getProperty("Target_Location_Master_EnrichFile")
          val Enrich_File_Name = Prop.getProperty("TargetPOS_inventory_Enrich_File_Name")
          val schemaString = Prop.getProperty("TargetPOS_inventory_schemaString")
          //val Target_POS_inventory_Incremental_Folder_Path = adl_path + Prop.getProperty("Target_POS_inventory_Incremental_Folder_Path") + "/" + Second_param
          //val Target_POS_inventory_Restatement_Folder_Path = adl_path + Prop.getProperty("Target_POS_inventory_Restatement_Folder_Path") + "/" + Second_param
          val Target_POS_inventory_UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Target_POS_inventory_UpdatedFiles_Folder_Path") + "/" + Second_param
          val FieldName = Prop.getProperty("Target_POS_inventory_Field_Name")
          val TableName = Prop.getProperty("Target_POS_inventory_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Target_POS_inventory_Table_path")
          val IncrementLogic: TargetPOS_INV = new TargetPOS_INV()
          //IncrementLogic.IncrementalTarget(Target_POS_inventory_Stage_File, Target_POS_inventory_EnrichFile, TargetPOS_Product, TargetPOS_Location, schemaString, Enrich_File_Name, Target_POS_inventory_Incremental_Folder_Path, Target_POS_inventory_Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          IncrementLogic.IncrementalTarget(Target_POS_inventory_Stage_File, Target_POS_inventory_EnrichFile, TargetPOS_Product, TargetPOS_Location, schemaString, Enrich_File_Name, Target_POS_inventory_UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)

        }

         case "Target_GRM" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Target_GRM", "inside case: Target_GRM")
          val Target_GRM_EnrichFile = adl_path + Prop.getProperty("Target_GRM_EnrichFile")
          val Target_GRM_Stage_File = adl_path + Prop.getProperty("Target_GRM_Stage_File")
          
          val STR_CNCPT = adl_path + Prop.getProperty("STR_CNCPT_EnrichFile")
          val GST_DEMOS = adl_path + Prop.getProperty("GST_DEMOS_EnrichFile")
          val STR_CURR = adl_path + Prop.getProperty("STR_CURR_EnrichFile")
          val ITEM_HIER = adl_path + Prop.getProperty("ITEM_HIER_EnrichFile")
          
          //val Target_GRM_Incremental_Folder_Path = adl_path + Prop.getProperty("Target_GRM_Incremental_Folder_Path") + "/" + Second_param
          //val Target_GRM_Restatement_Folder_Path = adl_path + Prop.getProperty("Target_GRM_Restatement_Folder_Path") + "/" + Second_param
          val Target_GRM_UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Target_GRM_UpdatedFiles_Folder_Path") + "/" + Second_param
          val FieldName = Prop.getProperty("Target_GRM_FieldName")
          val TableName = Prop.getProperty("Target_GRM_TableName")
          val Table_path = adl_path + Prop.getProperty("Target_GRM_Table_path")
          val Enrich_File_Name = Prop.getProperty("Target_GRM")
          val schemaString = Prop.getProperty("Target_GRM_Schema")
          val GRM: TargetPOS_GRM = new TargetPOS_GRM()
          //GRM.IncrementalTarget(Target_GRM_Stage_File, Target_GRM_EnrichFile, Target_GRM_PRODUCT, Target_GRM_LOCATION, schemaString, Enrich_File_Name, Target_GRM_Incremental_Folder_Path, Target_GRM_Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          GRM.IncrementalTargetGRM(Target_GRM_Stage_File, Target_GRM_EnrichFile,STR_CNCPT, GST_DEMOS, STR_CURR, ITEM_HIER, schemaString, Enrich_File_Name, Target_GRM_UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }        
       case "TargetCDT_Personal_Wash" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetCDT_Personal_Wash", "inside case: TargetCDT_Personal_Wash")
          val Enrich_File = adl_path + Prop.getProperty("Target_CDT_PERSONAL_WASH_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Target_CDT_PERSONAL_WASH_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Target_CDT_PERSONAL_WASH")
          val schemaString = Prop.getProperty("Target_CDT_PERSONAL_WASH_Schema")
          val col_Name = Prop.getProperty("Target_CDT_PERSONAL_WASH_ColName")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Target_CDT_PERSONAL_WASH_Incremental_Folder") + "/" + Second_param
          val targetCDT: TargetCDTcalculate = new TargetCDTcalculate()
          targetCDT.IncrementalTarget(Incremental_Folder_Path, Enrich_File, Stage_File, TargetPOS_Product, Enrich_File_Name, adl_path, schemaString, hadoopConf, hdfs, sqlContext, sc, col_Name, FolderPath_temp)
        }
        case "TargetCDT_Hair" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetCDT_Hair", "inside case: TargetCDT_Hair")
          val Enrich_File = adl_path + Prop.getProperty("Target_CDT_HAIR_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Target_CDT_HAIR_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Target_CDT_HAIR")
          val schemaString = Prop.getProperty("Target_CDT_HAIR_Schema")
          val col_Name = Prop.getProperty("Target_CDT_HAIR_ColName")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Target_CDT_HAIR_Incremental_Folder") + "/" + Second_param
          val targetCDT: TargetCDTcalculate = new TargetCDTcalculate()
          targetCDT.IncrementalTarget(Incremental_Folder_Path, Enrich_File, Stage_File, TargetPOS_Product, Enrich_File_Name, adl_path, schemaString, hadoopConf, hdfs, sqlContext, sc, col_Name, FolderPath_temp)

        }
        case "TargetCDT_Deo" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetCDT_Deo", "inside case: TargetCDT_Deo")
          val Enrich_File = adl_path + Prop.getProperty("Target_CDT_DEO_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Target_CDT_DEO_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Target_CDT_DEO")
          val schemaString = Prop.getProperty("Target_CDT_DEO_Schema")
          val col_Name = Prop.getProperty("Target_CDT_DEO_ColName")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Target_CDT_DEO_Incremental_Folder") + "/" + Second_param
          val targetCDT: TargetCDTcalculate = new TargetCDTcalculate()
          targetCDT.IncrementalTarget(Incremental_Folder_Path, Enrich_File, Stage_File, TargetPOS_Product, Enrich_File_Name, adl_path, schemaString, hadoopConf, hdfs, sqlContext, sc, col_Name, FolderPath_temp)

        }
        case "TargetCDT_Ice_Cream" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetCDT_Ice_Cream", "inside case: TargetCDT_Ice_Cream")
          val Enrich_File = adl_path + Prop.getProperty("Target_CDT_ICE_CREAM_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Target_CDT_ICE_CREAM_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Target_CDT_ICE_CREAM")
          val schemaString = Prop.getProperty("Target_CDT_ICE_CREAM_Schema")
          val col_Name = Prop.getProperty("Target_CDT_ICE_CREAM_ColName")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Target_CDT_ICE_CREAM_Incremental_Folder") + "/" + Second_param
          val targetCDT: TargetCDTcalculate = new TargetCDTcalculate()
          targetCDT.IncrementalTarget(Incremental_Folder_Path, Enrich_File, Stage_File, TargetPOS_Product, Enrich_File_Name, adl_path, schemaString, hadoopConf, hdfs, sqlContext, sc, col_Name, FolderPath_temp)

        }
        case "TargetCDT_Tea" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetCDT_Tea", "inside case: TargetCDT_Tea")
          val Enrich_File = adl_path + Prop.getProperty("Target_CDT_TEA_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Target_CDT_TEA_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Target_CDT_TEA")
          val schemaString = Prop.getProperty("Target_CDT_TEA_Schema")
          val col_Name = Prop.getProperty("Target_CDT_TEA_ColName")
          val TargetPOS_Product = adl_path + Prop.getProperty("Target_Product_Master_EnrichFile")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Target_CDT_TEA_Incremental_Folder") + "/" + Second_param
          val targetCDT: TargetCDTcalculate = new TargetCDTcalculate()
          targetCDT.IncrementalTarget(Incremental_Folder_Path, Enrich_File, Stage_File, TargetPOS_Product, Enrich_File_Name, adl_path, schemaString, hadoopConf, hdfs, sqlContext, sc, col_Name, FolderPath_temp)
        }

        case "Feature_Vision" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Feature_Vision", "inside case: Feature_Vision")
          val Enrich_File = adl_path + Prop.getProperty("Feature_Vision_EnrichFile")
          val Stage_File = adl_path + Prop.getProperty("Feature_Vision_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Feature_Vision")
          val schemaString = Prop.getProperty("Feature_Vision_Schema")
          val IncrementFolderPath = adl_path + Prop.getProperty("Feature_Vision_Incremental_Folder") + "/" + Second_param
          val Feature: ProcessCalculate = new ProcessCalculate()
          Feature.IncrementalFeature(Enrich_File, Stage_File, schemaString, Enrich_File_Name, Enrich_File_Name, IncrementFolderPath, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "PEA_PPG_Mapping" => {

          //logg.InsertLog(adl_path, FolderPath_temp, "PEA_PPG_Mapping", "inside case: PEA_PPG_Mapping")

          val Enrich_File = adl_path + Prop.getProperty("PEA_PPG_MAP_Enriched")
          val Stage_File = adl_path + Prop.getProperty("PEA_PPG_MAP_Stage")
          val Enrich_File_Name = Prop.getProperty("PEA_PPG_MAP")
          val schemaString = Prop.getProperty("PEA_PPG_MAP_Schema")
          val IncrementFolderPath = adl_path + Prop.getProperty("PEA_PPG_MAP_Incremental_Folder") + "/" + Second_param
          val Feature: ProcessCalculate = new ProcessCalculate()
          Feature.IncrementalFeature(Enrich_File, Stage_File, schemaString, Enrich_File_Name, Enrich_File_Name, IncrementFolderPath, adl_path, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Walmart_Assortment" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Assortment", "inside case: Walmart_Assortment")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_Assortment_Enrich")
          val Stage_File = adl_path + Prop.getProperty("Walmart_Assortment_Stage")
          val KeyField = Prop.getProperty("Walmart_Assortment_KeyField")
          val Walmart_ULHairOil_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Enrich_File")
          val WalmartStoreUL_ULHairOil_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_EnrichFile")
          val Assortment: AssortmentCalculation = new AssortmentCalculation()
          Assortment.IncrementCalculate(Enrich_File, Stage_File, Walmart_ULHairOil_ITEM_EnrichFile, WalmartStoreUL_ULHairOil_STORE_EnrichFile, KeyField, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp)
        }

        case "Nielsen_DHC_Market" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_DHC_Market", "inside case: Nielsen_DHC_Market")
          val NielsenDHCMarket_EnrichFile = adl_path + Prop.getProperty("NielsenDHCMarket_EnrichFile")
          val Nielsen_DHC_Market_Stage_File = adl_path + Prop.getProperty("Nielsen_DHC_Market_Stage_File")
          val NielsenDHCMarket_Schema = Prop.getProperty("NielsenDHCMarket_Schema")
          val Enrich_File_Name = Prop.getProperty("Nielsen_DHC_Market_Enrich_File_Name")
          val Nielsen_DHC_Market_IncrementFileName = Prop.getProperty("Nielsen_DHC_Market_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Nielsen_DHC_Market_Increment_Folder") + "/" + Second_param
          val NielsenDHC: NielsenCustomDHCMarket = new NielsenCustomDHCMarket()
          NielsenDHC.IncrementNielsenCustomDHCMarket(Nielsen_DHC_Market_Stage_File, NielsenDHCMarket_EnrichFile, sqlContext, sc, hadoopConf, hdfs, NielsenDHCMarket_Schema, adl_path, Enrich_File_Name, Nielsen_DHC_Market_IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }

        case "Nielsen_PPG" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_PPG", "inside case: Nielsen_PPG")
          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Nielsen_PPG_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Nielsen_PPG_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Nielsen_PPG_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Nielsen_PPG_Field_Name")
          val IncrementFileName = Prop.getProperty("Nielsen_PPG_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Nielsen_PPG_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Nielsen_PPG_EnrichFile")
          val Nielsen_PPG_Stage_File = adl_path + Prop.getProperty("Nielsen_PPG_Stage_File")
          val Nielsen_DHC_Market = adl_path + Prop.getProperty("NielsenDHCMarket_EnrichFile")
          val Nielsen_Product = adl_path + Prop.getProperty("NielsenProduct_EnrichFile")
          val Nielsen_Period = adl_path + Prop.getProperty("NielsenPeriod_EnrichFile")
          val Enrich_File_Name = Prop.getProperty("Nielsen_PPG_Enrich_File_Name")
          val schemaString = Prop.getProperty("Nielsen_PPG_Schema")
          val TableName = Prop.getProperty("Nielsen_PPG_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Nielsen_PPG_Table_path")
          val NielsenPPG: Nielsen_PPG = new Nielsen_PPG()
          //NielsenPPG.IncrementalNielsenPPG(Nielsen_PPG_Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_DHC_Market, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          NielsenPPG.IncrementalNielsenPPG(Nielsen_PPG_Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_DHC_Market, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }
        case "Nielsen_Period" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Period", "inside case: Nielsen_Period")
          val Nielsen_Period_Stage_File = adl_path + Prop.getProperty("Nielsen_Period_Stage_File")
          val NielsenPeriod_EnrichFile = adl_path + Prop.getProperty("NielsenPeriod_EnrichFile")
          val schemaString = Prop.getProperty("NielsenPeriod_Schema")
          val Enrich_File_Name = Prop.getProperty("NielsenPeriod_Enrich_File_Name")
          val IncrementFileName = Prop.getProperty("NielsenPeriod_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("NielsenPeriod_Incremental_Folder_Path") + "/" + Second_param
          val Nielsenperiod: NielsenPeriod = new NielsenPeriod()
          Nielsenperiod.IncrementNielsenPeriod(Nielsen_Period_Stage_File, NielsenPeriod_EnrichFile, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }
        case "Nielsen_Product" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Product", "inside case: Nielsen_Product")
          val Nielsen_Product_Stage_File = adl_path + Prop.getProperty("Nielsen_Product_Stage_File")
          val NielsenProduct_EnrichFile = adl_path + Prop.getProperty("NielsenProduct_EnrichFile")
          val schemaString = Prop.getProperty("NielsenProduct_Schema")
          val Enrich_File_Name = Prop.getProperty("NielsenProduct_Enrich_File_Name")
          val IncrementFileName = Prop.getProperty("NielsenProduct_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("NielsenProduct_Incremental_Folder_Path") + "/" + Second_param
          val nielsenproduct: NielsenProduct = new NielsenProduct()
          nielsenproduct.IncrementNielsenProduct(Nielsen_Product_Stage_File, NielsenProduct_EnrichFile, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }

        case "Walmart_Hairoil_POS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Hairoil_POS", "inside case: Walmart_Hairoil_POS")

          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_HairOil_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_HairOil_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_HairOil_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Walmart_POS_HairOil_FieldName")
          val IncrementFileName = Prop.getProperty("Walmart_POS_HairOil_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Walmart_POS_HairOil_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_POS_HairOil_EnrichFile")
          val Walmart_POS_HAIROIL_Stage_File = adl_path + Prop.getProperty("Walmart_POS_HAIROIL_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Walmart_POS_HairOil_Enrich_File_Name")
          val schemaString = Prop.getProperty("Walmart_ULHairOil_POS_Schema")
          val TableName = Prop.getProperty("Walmart_POS_HairOil_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Walmart_POS_HairOil_Table_Path")
          val Walmart_ULHairOil_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_ULHairOil_ITEM_Enrich_File")
          val WalmartStoreUL_ULHairOil_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_EnrichFile")
          val WalmartHairoil: WalmartPOS_All = new WalmartPOS_All()
          //WalmartHairoil.IncrementWalmart(Walmart_POS_HAIROIL_Stage_File, Enrich_File, Walmart_ULHairOil_ITEM_EnrichFile, WalmartStoreUL_ULHairOil_STORE_EnrichFile, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          WalmartHairoil.IncrementWalmart(Walmart_POS_HAIROIL_Stage_File, Enrich_File, Walmart_ULHairOil_ITEM_EnrichFile, WalmartStoreUL_ULHairOil_STORE_EnrichFile, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }
        case "Walmart_Icecream_POS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Icecream_POS", "inside case: Walmart_Icecream_POS")
          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Walmart_POS_ICECREAM_FieldName")
          val IncrementFileName = Prop.getProperty("Walmart_POS_ICECREAM_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Walmart_POS_ICECREAM_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_EnrichFile")
          val Walmart_POS_ICECREAM_Stage_File = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Walmart_POS_ICECREAM_Enrich_File_Name")
          val schemaString = Prop.getProperty("Walmart_ICECREAM_POS_Schema")
          val TableName = Prop.getProperty("Walmart_POS_ICECREAM_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Walmart_POS_ICECREAM_Table_Path")
          val Walmart_ICECREAM_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_ICECREAM_ITEM_Enrich_File")
          val WalmartStoreUL_ICECREAM_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_EnrichFile")
          val WalmartIceCreamoil: WalmartPOS_All = new WalmartPOS_All()
          //WalmartIceCreamoil.IncrementWalmart(Walmart_POS_ICECREAM_Stage_File, Enrich_File, Walmart_ICECREAM_ITEM_EnrichFile, WalmartStoreUL_ICECREAM_STORE_EnrichFile, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          WalmartIceCreamoil.IncrementWalmart(Walmart_POS_ICECREAM_Stage_File, Enrich_File, Walmart_ICECREAM_ITEM_EnrichFile, WalmartStoreUL_ICECREAM_STORE_EnrichFile, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }
        case "Walmart_SASC_POS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_SASC_POS", "inside case: Walmart_SASC_POS")
          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Walmart_SASC_POS_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Walmart_SASC_POS_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Walmart_SASC_POS_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Walmart_SASC_POS_FieldName")
          val IncrementFileName = Prop.getProperty("Walmart_SASC_POS_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Walmart_SASC_POS_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_SASC_POS_EnrichFile")
          val Walmart_POS_SASC_Stage_File = adl_path + Prop.getProperty("Walmart_POS_SASC_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Walmart_SASC_POS_Enrich_File_Name")
          val schemaString = Prop.getProperty("Walmart_SASC_POS_Schema")
          val TableName = Prop.getProperty("Walmart_SASC_POS_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Walmart_SASC_POS_Table_Path")
          val Walmart_SASC_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_USASCC_ITEM_Enrich_File")
          val WalmartStoreUL_SASC_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_SASC_STORE_EnrichFile")
          val WalmartSASC: WalmartPOS_USASCC = new WalmartPOS_USASCC
          //WalmartSASC.IncrementWalmart(Walmart_POS_SASC_Stage_File, Enrich_File, Walmart_SASC_ITEM_EnrichFile, WalmartStoreUL_SASC_STORE_EnrichFile, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          WalmartSASC.IncrementWalmart(Walmart_POS_SASC_Stage_File, Enrich_File, Walmart_SASC_ITEM_EnrichFile, WalmartStoreUL_SASC_STORE_EnrichFile, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)

        }
        case "Walmart_Spread_POS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Spread_POS", "inside case: Walmart_Spread_POS")
          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Walmart_POS_SPREADS_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Walmart_POS_SPREADS_FieldName")
          val IncrementFileName = Prop.getProperty("Walmart_POS_SPREADS_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Walmart_POS_SPREADS_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_POS_SPREADS_EnrichFile")
          val Walmart_POS_SPREADS_Stage_File = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Walmart_POS_SPREADS_Enrich_File_Name")
          val schemaString = Prop.getProperty("Walmart_SPREADS_POS_Schema")
          val TableName = Prop.getProperty("Walmart_POS_SPREADS_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Walmart_POS_SPREADS_Table_Path")
          val Walmart_SPREADS_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_SPREADS_ITEM_Enrich_File")
          val WalmartStoreUL_SPREADS_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_SPREADS_STORE_EnrichFile")
          val WalmartSpread: WalmartPOS_All = new WalmartPOS_All
          //WalmartSpread.IncrementWalmart(Walmart_POS_SPREADS_Stage_File, Enrich_File, Walmart_SPREADS_ITEM_EnrichFile, WalmartStoreUL_SPREADS_STORE_EnrichFile, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          WalmartSpread.IncrementWalmart(Walmart_POS_SPREADS_Stage_File, Enrich_File, Walmart_SPREADS_ITEM_EnrichFile, WalmartStoreUL_SPREADS_STORE_EnrichFile, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }
        case "Walmart_Tea_POS" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Tea_POS", "inside case: Walmart_Tea_POS")
          //val Incremental_Folder_Path = adl_path + Prop.getProperty("Walmart_TEA_POS_Increment_Folder") + "/" + Second_param
          //val Restatement_Folder_Path = adl_path + Prop.getProperty("Walmart_TEA_POS_Restatement_Folder") + "/" + Second_param
          val UpdatedFiles_Folder_Path = adl_path + Prop.getProperty("Walmart_TEA_POS_UpdatedFiles_Folder") + "/" + Second_param
          val FieldName = Prop.getProperty("Walmart_TEA_POS_FieldName")
          val IncrementFileName = Prop.getProperty("Walmart_TEA_POS_IncrementFileName")
          val RestatementFileName = Prop.getProperty("Walmart_TEA_POS_RestatementFileName")
          val Enrich_File = adl_path + Prop.getProperty("Walmart_TEA_POS_EnrichFile")
          val Walmart_Tea_POS_Stage_File = adl_path + Prop.getProperty("Walmart_POS_TEA_Stage_File")
          val Enrich_File_Name = Prop.getProperty("Walmart_TEA_POS_Enrich_File_Name")
          val schemaString = Prop.getProperty("Walmart_TEA_POS_Schema")
          val TableName = Prop.getProperty("Walmart_TEA_POS_Table_Name")
          val Table_path = adl_path + Prop.getProperty("Walmart_TEA_POS_Table_Path")
          val Walmart_SPREADS_ITEM_EnrichFile = adl_path + Prop.getProperty("Walmart_TEA_ITEM_Enrich_File")
          val WalmartStoreUL_SPREADS_STORE_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_TEA_STORE_EnrichFile")
          val WalmartTea: WalmartPOS_All = new WalmartPOS_All
          //WalmartTea.IncrementWalmart(Walmart_Tea_POS_Stage_File, Enrich_File, Walmart_SPREADS_ITEM_EnrichFile, WalmartStoreUL_SPREADS_STORE_EnrichFile, schemaString, Enrich_File_Name, Incremental_Folder_Path, Restatement_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
          WalmartTea.IncrementWalmart(Walmart_Tea_POS_Stage_File, Enrich_File, Walmart_SPREADS_ITEM_EnrichFile, WalmartStoreUL_SPREADS_STORE_EnrichFile, schemaString, Enrich_File_Name, UpdatedFiles_Folder_Path, FieldName, adl_path, sqlContext, sc, hadoopConf, hdfs, FolderPath_temp, Stage_To_Enrich_Error)
        }

        case "Walmart_Store_HairOil" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Store_HairOil", "inside case: Walmart_Store_HairOil")
          val WalmartStoreUL_Stage_File = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_Stage_File")
          val WalmartStoreUL_EnrichFile = adl_path + Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_EnrichFile")
          val WalmartStoreUL_ULHairOil_STORE_Schema = Prop.getProperty("WalmartStoreUL_ULHairOil_STORE_Schema")
          val Enrich_File_Name = Prop.getProperty("File_ULHairOil_STORE")
          val IncrementFileName = Prop.getProperty("IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Incremental_Folder_Path") + "/" + Second_param
          val WalmartStoreHairOil: WalmartStoreUL = new WalmartStoreUL()
          WalmartStoreHairOil.IncrementWalmartStoreUL(WalmartStoreUL_Stage_File, WalmartStoreUL_EnrichFile, sqlContext, sc, hadoopConf, hdfs, adl_path, WalmartStoreUL_ULHairOil_STORE_Schema, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }

        case "Walmart_Store_SASC" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Store_SASC", "inside case: Walmart_Store_SASC")
          val WalmartStoreUL_Stage_File01 = adl_path + Prop.getProperty("WalmartStoreUL_SASC_STORE_Stage_File")
          val WalmartStoreUL_EnrichFile01 = adl_path + Prop.getProperty("WalmartStoreUL_SASC_STORE_EnrichFile")
          val WalmartStoreUL_SASC_STORE_Schema = Prop.getProperty("WalmartStoreUL_SASC_STORE_Schema")
          val Enrich_File_Name = Prop.getProperty("File_SASC_STORE")
          val IncrementFileName = Prop.getProperty("IncrementFileName_SASC_STORE")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Incremental_Folder_Path_SASC_STORE") + "/" + Second_param
          val WalmartStoreSASC: WalmartStoreUL = new WalmartStoreUL()
          WalmartStoreSASC.IncrementWalmartStoreUL(WalmartStoreUL_Stage_File01, WalmartStoreUL_EnrichFile01, sqlContext, sc, hadoopConf, hdfs, adl_path, WalmartStoreUL_SASC_STORE_Schema, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }

        case "Walmart_Store_Tea" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Store_Tea", "inside case: Walmart_Store_Tea")
          val WalmartStoreUL_Stage_File02 = adl_path + Prop.getProperty("WalmartStoreUL_TEA_STORE_Stage_File")
          val WalmartStoreUL_EnrichFile02 = adl_path + Prop.getProperty("WalmartStoreUL_TEA_STORE_EnrichFile")
          val WalmartStoreUL_Tea_STORE_Schema = Prop.getProperty("WalmartStoreUL_TEA_STORE_Schema")
          val Enrich_File_Name = Prop.getProperty("File_TEA_STORE")
          val IncrementFileName = Prop.getProperty("IncrementFileName_TEA_STORE")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Incremental_Folder_Path_TEA_STORE") + "/" + Second_param
          val WalmartStoreTea: WalmartStoreUL = new WalmartStoreUL()
          WalmartStoreTea.IncrementWalmartStoreUL(WalmartStoreUL_Stage_File02, WalmartStoreUL_EnrichFile02, sqlContext, sc, hadoopConf, hdfs, adl_path, WalmartStoreUL_Tea_STORE_Schema, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }

        case "Walmart_Store_Spreads" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Store_Spreads", "inside case: Walmart_Store_Spreads")
          val WalmartStoreUL_Stage_File03 = adl_path + Prop.getProperty("WalmartStoreUL_SPREADS_STORE_Stage_File")
          val WalmartStoreUL_EnrichFile03 = adl_path + Prop.getProperty("WalmartStoreUL_SPREADS_STORE_EnrichFile")
          val WalmartStoreUL_Tea_STORE_Schema = Prop.getProperty("WalmartStoreUL_SPREADS_STORE_Schema")
          val Enrich_File_Name = Prop.getProperty("File_SPREADS_STORE")
          val IncrementFileName = Prop.getProperty("IncrementFileName_SPREADS_STORE")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Incremental_Folder_Path_SPREADS_STORE") + "/" + Second_param
          val WalmartStoreSpreads: WalmartStoreUL = new WalmartStoreUL()
          WalmartStoreSpreads.IncrementWalmartStoreUL(WalmartStoreUL_Stage_File03, WalmartStoreUL_EnrichFile03, sqlContext, sc, hadoopConf, hdfs, adl_path, WalmartStoreUL_Tea_STORE_Schema, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }
        case "Walmart_Store_IceCream" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Walmart_Store_IceCream", "inside case: Walmart_Store_IceCream")
          val WalmartStoreUL_Stage_File04 = adl_path + Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_Stage_File")
          val WalmartStoreUL_EnrichFile04 = adl_path + Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_EnrichFile")
          val WalmartStoreUL_Tea_STORE_Schema = Prop.getProperty("WalmartStoreUL_ICECREAM_STORE_Schema")
          val Enrich_File_Name = Prop.getProperty("File_ICECREAM_STORE")
          val IncrementFileName = Prop.getProperty("IncrementFileName_ICECREAM_STORE")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Incremental_Folder_Path_ICECREAM_STORE") + "/" + Second_param
          val WalmartStoreIceCream: WalmartStoreUL = new WalmartStoreUL()
          WalmartStoreIceCream.IncrementWalmartStoreUL(WalmartStoreUL_Stage_File04, WalmartStoreUL_EnrichFile04, sqlContext, sc, hadoopConf, hdfs, adl_path, WalmartStoreUL_Tea_STORE_Schema, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }
        case "PEA" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "PEA", "inside case: PEA")
          val PEA_Stage_File = adl_path + Prop.getProperty("PEA_Stage_File")
          val PEA_EnrichFile = adl_path + Prop.getProperty("PEA_EnrichFile")
          val schemaString = Prop.getProperty("PEA_Schema")
          val Enrich_File_Name = Prop.getProperty("PEA_Enrich_File_Name")
          val IncrementFileName = Prop.getProperty("PEA_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("PEA_Incremental_Folder_Path") + "/" + Second_param
          val Mapping_UPC_PPG = adl_path + Prop.getProperty("Mapping_UPC_PPG_Enriched")
          val PEA1: PEA = new PEA()
          PEA1.IncrementPEA(PEA_Stage_File, PEA_EnrichFile, sqlContext, sc, hadoopConf, hdfs, adl_path, schemaString, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, Mapping_UPC_PPG, FolderPath_temp)
        }
        case "TDLinx" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TDLinx", "inside case: TDLinx")
          val StageFile = adl_path + Prop.getProperty("TDLinks_Stage_File")
          val EnrichedFile = adl_path + Prop.getProperty("TDLinks_EnrichFile")
          val schemaString = Prop.getProperty("TDLinks_Schema")
          val Enrich_File_Name = Prop.getProperty("TDLinks_Enrich_File_Name")
          val IncrementFileName = Prop.getProperty("TDLinks_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("TDLinks_Incremental_Folder_Path") + "/" + Second_param
          val TDLinks1: TDLinx = new TDLinx
          TDLinks1.IncrementTDLinx(StageFile, EnrichedFile, sqlContext, sc, hadoopConf, hdfs, schemaString, adl_path, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, FolderPath_temp)
        }
        case "Nielsen_Demo" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Demo", "inside case: Nielsen_Demo")
          val Demographic_Stage_File = adl_path + Prop.getProperty("Demographic_Stage_File")
          val Demographic_EnrichFile = adl_path + Prop.getProperty("Demographic_EnrichFile")
          val schemaString = Prop.getProperty("Demographic_Schema")
          val Enrich_File_Name = Prop.getProperty("Demographic_Enrich_File_Name")
          val IncrementFileName = Prop.getProperty("Demographic_IncrementFileName")
          val Incremental_Folder_Path = adl_path + Prop.getProperty("Demographic_Incremental_Folder_Path") + "/" + Second_param
          val TDLinx_Enriched = adl_path + Prop.getProperty("TDLinks_EnrichFile")
          val Demo: NielsenDemographic = new NielsenDemographic
          Demo.IncrementDemographic(Demographic_Stage_File, Demographic_EnrichFile, sqlContext, sc, hadoopConf, hdfs, adl_path, schemaString, Enrich_File_Name, IncrementFileName, Incremental_Folder_Path, TDLinx_Enriched, FolderPath_temp)
        }
        case "WMPOS_Tea_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WMPOS_Tea_MissingWk", "inside case: WMPOS_Tea_MissingWk")
          val WMPOS_Tea_MissingWk_EnrichFile = adl_path + Prop.getProperty("WMPOS_Tea_MissingWk_Enrich_File")
          val WMPOS_Tea_MissingWk_ColName = Prop.getProperty("WMPOS_Tea_MissingWk_Col_Name")
          val WMPOS_Tea_MissingWk_DestFile = adl_path + Prop.getProperty("WMPOS_Tea_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(WMPOS_Tea_MissingWk_EnrichFile, WMPOS_Tea_MissingWk_ColName, adl_path, WMPOS_Tea_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "WMPOS_HairOil_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WMPOS_HairOil_MissingWk", "inside case: WMPOS_HairOil_MissingWk")
          val WMPOS_HairOil_MissingWk_EnrichFile = adl_path + Prop.getProperty("WMPOS_HairOil_MissingWk_Enrich_File")
          val WMPOS_HairOil_MissingWk_ColName = Prop.getProperty("WMPOS_HairOil_MissingWk_Col_Name")
          val WMPOS_HairOil_MissingWk_DestFile = adl_path + Prop.getProperty("WMPOS_HairOil_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(WMPOS_HairOil_MissingWk_EnrichFile, WMPOS_HairOil_MissingWk_ColName, adl_path, WMPOS_HairOil_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "WMPOS_IceCream_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WMPOS_IceCream_MissingWk", "inside case: WMPOS_IceCream_MissingWk")
          val WMPOS_IceCream_MissingWk_EnrichFile = adl_path + Prop.getProperty("WMPOS_IceCream_MissingWk_Enrich_File")
          val WMPOS_IceCream_MissingWk_ColName = Prop.getProperty("WMPOS_IceCream_MissingWk_Col_Name")
          val WMPOS_IceCream_MissingWk_DestFile = adl_path + Prop.getProperty("WMPOS_IceCream_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(WMPOS_IceCream_MissingWk_EnrichFile, WMPOS_IceCream_MissingWk_ColName, adl_path, WMPOS_IceCream_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "WMPOS_Spreads_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WMPOS_Spreads_MissingWk", "inside case: WMPOS_Spreads_MissingWk")
          val WMPOS_Spreads_MissingWk_EnrichFile = adl_path + Prop.getProperty("WMPOS_Spreads_MissingWk_Enrich_File")
          val WMPOS_Spreads_MissingWk_ColName = Prop.getProperty("WMPOS_Spreads_MissingWk_Col_Name")
          val WMPOS_Spreads_MissingWk_DestFile = adl_path + Prop.getProperty("WMPOS_Spreads_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(WMPOS_Spreads_MissingWk_EnrichFile, WMPOS_Spreads_MissingWk_ColName, adl_path, WMPOS_Spreads_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "WMPOS_SASC_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "WMPOS_SASC_MissingWk", "inside case: WMPOS_SASC_MissingWk")
          val WMPOS_SASC_MissingWk_EnrichFile = adl_path + Prop.getProperty("WMPOS_SASC_MissingWk_Enrich_File")
          val WMPOS_SASC_MissingWk_ColName = Prop.getProperty("WMPOS_SASC_MissingWk_Col_Name")
          val WMPOS_SASC_MissingWk_DestFile = adl_path + Prop.getProperty("WMPOS_SASC_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(WMPOS_SASC_MissingWk_EnrichFile, WMPOS_SASC_MissingWk_ColName, adl_path, WMPOS_SASC_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Nielsen_Scan_UPC_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_Scan_UPC_MissingWk", "inside case: Nielsen_Scan_UPC_MissingWk")
          val Nielsen_Scan_UPC_MissingWk_EnrichFile = adl_path + Prop.getProperty("Nielsen_Scan_UPC_MissingWk_Enrich_File")
          val Nielsen_Scan_UPC_MissingWk_ColName = Prop.getProperty("Nielsen_Scan_UPC_MissingWk_Col_Name")
          val Nielsen_Scan_UPC_MissingWk_DestFile = adl_path + Prop.getProperty("Nielsen_Scan_UPC_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(Nielsen_Scan_UPC_MissingWk_EnrichFile, Nielsen_Scan_UPC_MissingWk_ColName, adl_path, Nielsen_Scan_UPC_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "TargetPOS_Sales_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_Sales_MissingWk", "inside case: TargetPOS_Sales_MissingWk")
          val TargetPOS_Sales_MissingWk_EnrichFile = adl_path + Prop.getProperty("TargetPOS_Sales_MissingWk_Enrich_File")
          val TargetPOS_Sales_MissingWk_ColName = Prop.getProperty("TargetPOS_Sales_MissingWk_Col_Name")
          val TargetPOS_Sales_MissingWk_DestFile = adl_path + Prop.getProperty("TargetPOS_Sales_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(TargetPOS_Sales_MissingWk_EnrichFile, TargetPOS_Sales_MissingWk_ColName, adl_path, TargetPOS_Sales_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "TargetPOS_inventory_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "TargetPOS_inventory_MissingWk", "inside case: TargetPOS_inventory_MissingWk")
          val TargetPOS_inventory_MissingWk_EnrichFile = adl_path + Prop.getProperty("TargetPOS_inventory_MissingWk_Enrich_File")
          val TargetPOS_inventory_MissingWk_ColName = Prop.getProperty("TargetPOS_inventory_MissingWk_Col_Name")
          val TargetPOS_inventory_MissingWk_DestFile = adl_path + Prop.getProperty("TargetPOS_inventory_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(TargetPOS_inventory_MissingWk_EnrichFile, TargetPOS_inventory_MissingWk_ColName, adl_path, TargetPOS_inventory_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Target_GRM_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Target_GRM_MissingWk", "inside case: Target_GRM_MissingWk")
          val Target_GRM_MissingWk_EnrichFile = adl_path + Prop.getProperty("Target_GRM_MissingWk_Enrich_File")
          val Target_GRM_MissingWk_ColName = Prop.getProperty("Target_GRM_MissingWk_Col_Name")
          val Target_GRM_MissingWk_DestFile = adl_path + Prop.getProperty("Target_GRM_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(Target_GRM_MissingWk_EnrichFile, Target_GRM_MissingWk_ColName, adl_path, Target_GRM_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case "Nielsen_PPG_MissingWk" => {
          //logg.InsertLog(adl_path, FolderPath_temp, "Nielsen_PPG_MissingWk", "inside case: Nielsen_PPG_MissingWk")
          val Nielsen_PPG_MissingWk_EnrichFile = adl_path + Prop.getProperty("Nielsen_PPG_MissingWk_Enrich_File")
          val Nielsen_PPG_MissingWk_ColName = Prop.getProperty("Nielsen_PPG_MissingWk_Col_Name")
          val Nielsen_PPG_MissingWk_DestFile = adl_path + Prop.getProperty("Nielsen_PPG_MissingWk_Dest_File") + "/" + Second_param
          val MissingWeek: MissingWeekData = new MissingWeekData()
          MissingWeek.POSMissingWeeks(Nielsen_PPG_MissingWk_EnrichFile, Nielsen_PPG_MissingWk_ColName, adl_path, Nielsen_PPG_MissingWk_DestFile, hadoopConf, hdfs, sqlContext, sc, FolderPath_temp)
        }
        case other => {
          //logg.InsertLog(adl_path, FolderPath_temp, "other", "inside case: other")
        }

      }
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    } finally {
      sc.stop()
    }

  }

}