package com.DataIQ.StageToEnrichProcessCalculate;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

public class Nielsen_PPGTest extends SharedJavaSparkContext {

	@Test
	public void testIncrementalNielsenPPG() throws Exception {
		Nielsen_PPG ppg = new Nielsen_PPG();
		
		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
		
		String Error_Folder = "./TestData/Error";
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		Boolean flag = false;
		String Stage_File = "/TestData/NLS_PPG_20170801_001 (1).csv";
		String Enrich_File = "./TestData/NielsenPPG/BigFile";
		//String Incremental_Folder_Path = "./TestData/NielsenPPG/Increment";
		//String Restatement_Folder_Path = "./TestData/NielsenPPG/Restatement";
		String UpdateFiles_Folder_Path = "./TestData/NielsenPPG/UpdatedFiles";
		String Nielsen_Product = "./TestData/NielsenProduct.csv";
		String Nielsen_Period = "./TestData/NielsenPeriod_20170720_1500550030528.csv";
		String Nielsen_PPG_Schema = "GEO,Description,Tag,RCategory,WeekEnding,Dollars,Units,BaseDollars,BaseUnits,PACV_FeatAndDisp,PACV_FeatWODisp,PACV_DispWOFeat,PACV_Discount,Dollars_FeatAndDisp,Dollars_Feature,Dollars_Display,Dollars_TPR,Units_FeatAndDisp,Units_Feature,Units_Display,Units_TPR,Units_EQ,BaseUnits_Feature,BaseDollars_Feature,BaseUnits_Display,BaseDollars_Display,BaseUnits_FeatAndDisp,BaseDollars_FeatAndDisp,BaseUnits_TPR,BaseDollars_TPR,EQUnitConversion,BaseUnits_EQ,Units_EQ_Feature,Units_EQ_Display,Units_EQ_FeatAndDisp,Units_EQ_TPR,BaseUnits_EQ_Feature,BaseUnits_EQ_Display,BaseUnits_EQ_FeatAndDisp,BaseUnits_EQ_TPR,Units_AnyPromo,Dollars_AnyPromo,BaseDollars_AnyPromo,BaseUnits_AnyPromo,Units_EQ_AnyPromo,BaseUnits_EQ_AnyPromo,Units_NoPromo,Dollars_NoPromo,BaseDollars_NoPromo,BaseUnits_NoPromo,Units_EQ_NoPromo,BaseUnits_EQ_NoPromo,Units_FeatAndOrDisp,Dollars_FeatAndOrDisp,BaseDollars_FeatAndOrDisp,BaseUnits_FeatAndOrDisp,Units_EQ_FeatAndOrDisp,BaseUnits_EQ_FeatAndOrDisp,ACV";
		
		
		//flag = ppg.IncrementalNielsenPPG(Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_Period, Nielsen_PPG_Schema, "Nielsen_ppg", Incremental_Folder_Path, Restatement_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, Error_Folder, Error_Folder);
		flag = ppg.IncrementalNielsenPPG(Stage_File, Enrich_File, Nielsen_Product, Nielsen_Period, Nielsen_Period, Nielsen_PPG_Schema, "Nielsen_ppg", UpdateFiles_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, Error_Folder, Error_Folder);
		
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		assertFalse(flag);
	}

}
