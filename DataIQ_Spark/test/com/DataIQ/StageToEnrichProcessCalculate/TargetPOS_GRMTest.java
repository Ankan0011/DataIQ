package com.DataIQ.StageToEnrichProcessCalculate;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

public class TargetPOS_GRMTest extends SharedJavaSparkContext{

	@Test
	public void testIncrementalTarget() throws Exception {
		
		TargetPOS_GRM tgt = new TargetPOS_GRM();
		
		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
		
		String Error_Folder = "./TestData/Error";
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		Boolean flag = false;
		String Stage_File = "/TestData/NLS_PPG_20170801_001 (1).csv";
		String Enrich_File = "./TestData/TargetGRM/BigFile";
		//String Incremental_Folder_Path = "./TestData/TargetGRM/Increment";
		//String Restatement_Folder_Path = "./TestData/TargetGRM/Restatement";
		String UpdatedFiles_Folder_Path = "";
		String TargetGRM_Product = "./TestData/TGT_PRODUCT_MSTR.csv";
		String TargetGRM_Location = "./TestData/TGT_PRODUCT_MSTR.csv";
		String Target_GRM_Schema = "ConsumerID,TransDate,TripID,LineType,TripClassCode,SalesTypeCode,ScanOrder,ProductNbr,LocationNbr,NetAmount,Quantity";
		
		//flag = tgt.IncrementalTarget(Stage_File, Enrich_File, TargetGRM_Product, TargetGRM_Location, Target_GRM_Schema, "TargetGRM", Incremental_Folder_Path, Restatement_Folder_Path, "ProductNbr", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
		flag = tgt.IncrementalTargetGRM(Stage_File, Enrich_File, TargetGRM_Product,TargetGRM_Product,TargetGRM_Product, TargetGRM_Location, Target_GRM_Schema, "TargetGRM", UpdatedFiles_Folder_Path, "ProductNbr", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
	
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		assertFalse(flag);
	}

}
