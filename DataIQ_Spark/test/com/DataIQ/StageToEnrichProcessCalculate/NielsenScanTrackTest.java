package com.DataIQ.StageToEnrichProcessCalculate;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

public class NielsenScanTrackTest extends SharedJavaSparkContext{

	@Test
	public void testIncrementNielsenCustomScanUPC() throws Exception{
		NielsenScanTrack nst = new NielsenScanTrack();
		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
		
		String Error_Folder = "./TestData/Error";
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		Boolean flag = false;
		String Stage_File = "/TestData/NLS_SYN_20170915.csv";
		String Enrich_File = "./TestData/NielsenScan/BigFile";
		//String Incremental_Folder_Path = "./TestData/NielsenScan/Increment";
		//String Restatement_Folder_Path = "./TestData/NielsenScan/Restatement";
		String UpdatedFiles_Folder_Path = "./TestData/NielsenScan/UpdatedFiles";
		String Nielsen_Product = "./TestData/NielsenProduct.csv";
		String Nielsen_Period = "./TestData/NielsenPeriod_20170720_1500550030528.csv";
		String Nielsen_Scan_UPC_File_Schema = "GEO,UPC,WeekEnding,BaseDollars,BaseDollars_AnyPromo,BaseDollars_Display,BaseDollars_FeatAndDisp,BaseDollars_FeatAndOrDisp,BaseDollars_Feature,BaseDollars_NoPromo,BaseDollars_TPR,BaseUnits,BaseUnits_AnyPromo,BaseUnits_Display,BaseUnits_EQ,BaseUnits_EQ_AnyPromo,BaseUnits_EQ_Display,BaseUnits_EQ_FeatAndDisp,BaseUnits_EQ_FeatAndOrDisp,BaseUnits_EQ_Feature,BaseUnits_EQ_NoPromo,BaseUnits_EQ_TPR,BaseUnits_FeatAndDisp,BaseUnits_FeatAndOrDisp,BaseUnits_Feature,BaseUnits_NoPromo,BaseUnits_TPR,Dollars,Dollars_AnyPromo,Dollars_Display,Dollars_FeatAndDisp,Dollars_FeatAndOrDisp,Dollars_Feature,Dollars_NoPromo,Dollars_TPR,PACV_Discount,PACV_DispWOFeat,PACV_FeatAndDisp,PACV_FeatWODisp,Units,Units_AnyPromo,Units_Display,Units_EQ,Units_EQ_AnyPromo,Units_EQ_Display,Units_EQ_FeatAndDisp,Units_EQ_FeatAndOrDisp,Units_EQ_Feature,Units_EQ_NoPromo,Units_EQ_TPR,Units_FeatAndDisp,Units_FeatAndOrDisp,Units_Feature,Units_NoPromo,Units_TPR,ACV,Max_PeriodEnd";

		
		//flag = nst.IncrementNielsenCustomScanUPC(Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_Period, Nielsen_Scan_UPC_File_Schema, "NielsenScantrack", Incremental_Folder_Path, Restatement_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, Error_Folder, Error_Folder);
		flag = nst.IncrementNielsenCustomScanUPC(Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_Period, Nielsen_Scan_UPC_File_Schema, "NielsenScantrack", UpdatedFiles_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, Error_Folder, Error_Folder);
	
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		assertFalse(flag);
	}

}
