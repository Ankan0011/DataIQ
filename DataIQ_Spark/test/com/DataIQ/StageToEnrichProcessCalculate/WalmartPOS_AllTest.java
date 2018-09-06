package com.DataIQ.StageToEnrichProcessCalculate;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

public class WalmartPOS_AllTest extends SharedJavaSparkContext{

	@Test
	public void testIncrementWalmart() throws Exception {
		WalmartPOS_All ws = new WalmartPOS_All();
		
		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
		
		String Error_Folder = "./TestData/Error";
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		Boolean flag = false;
		String Stage_File = "/TestData/HairOil-Incremental.csv";
		String Enrich_File = "./TestData/Walmart/BigFile";
		//String Incremental_Folder_Path = "./TestData/Walmart/Increment";
		//String Restatement_Folder_Path = "./TestData/Walmart/Restatement";
		String UpdatedFiles_Folder_Path = "";
		String Walmart_Item = "./TestData/TGT_PRODUCT_MSTR.csv";
		String Walmart_Store = "./TestData/TGT_PRODUCT_MSTR.csv";
		String Walmart_Schema = "GEO,Description,Tag,RCategory,WeekEnding,Dollars,Units,BaseDollars,BaseUnits,PACV_FeatAndDisp,PACV_FeatWODisp,PACV_DispWOFeat,PACV_Discount,Dollars_FeatAndDisp,Dollars_Feature,Dollars_Display,Dollars_TPR,Units_FeatAndDisp,Units_Feature,Units_Display,Units_TPR,Units_EQ,BaseUnits_Feature,BaseDollars_Feature,BaseUnits_Display,BaseDollars_Display,BaseUnits_FeatAndDisp,BaseDollars_FeatAndDisp,BaseUnits_TPR,BaseDollars_TPR,EQUnitConversion,BaseUnits_EQ,Units_EQ_Feature,Units_EQ_Display,Units_EQ_FeatAndDisp,Units_EQ_TPR,BaseUnits_EQ_Feature,BaseUnits_EQ_Display,BaseUnits_EQ_FeatAndDisp,BaseUnits_EQ_TPR,Units_AnyPromo,Dollars_AnyPromo,BaseDollars_AnyPromo,BaseUnits_AnyPromo,Units_EQ_AnyPromo,BaseUnits_EQ_AnyPromo,Units_NoPromo,Dollars_NoPromo,BaseDollars_NoPromo,BaseUnits_NoPromo,Units_EQ_NoPromo,BaseUnits_EQ_NoPromo,Units_FeatAndOrDisp,Dollars_FeatAndOrDisp,BaseDollars_FeatAndOrDisp,BaseUnits_FeatAndOrDisp,Units_EQ_FeatAndOrDisp,BaseUnits_EQ_FeatAndOrDisp,ACV";
		
		//flag = ws.IncrementWalmart(Stage_File, Enrich_File, Walmart_Item, Walmart_Store, Walmart_Schema, "Walmart", Incremental_Folder_Path, Restatement_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
		flag = ws.IncrementWalmart(Stage_File, Enrich_File, Walmart_Item, Walmart_Store, Walmart_Schema, "Walmart", UpdatedFiles_Folder_Path, "GEO", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
	
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		assertFalse(flag);
	}

}
