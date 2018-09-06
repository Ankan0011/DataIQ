package com.DataIQ.StageToEnrichProcessCalculate;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

public class TargetPOS_INVTest extends SharedJavaSparkContext{

	@Test
	public void testIncrementalTarget() throws Exception 
	{
		TargetPOS_INV tis = new TargetPOS_INV();

		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
		
		String Error_Folder = "./TestData/Error";
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		Boolean flag = false;
		String Stage_File = "/TestData/NLS_PPG_20170801_001 (1).csv";
		String Enrich_File = "./TestData/TargetInv/BigFile";
		//String Incremental_Folder_Path = "./TestData/TargetInv/Increment";
		//String Restatement_Folder_Path = "./TestData/TargetInv/Restatement";
		String UpdatedFiles_Folder_Path = "";
		String TargetPOS_Product = "./TestData/TGT_PRODUCT_MSTR.csv";
		String TargetPOS_Location = "./TestData/TGT_PRODUCT_MSTR.csv";
		String TargetPOS_inventory_schemaString = "TransWeek,LocationNbr,ProductNbr,EOHSales,EOHUnits,InStockPct,OutStockPct,BOHSales,BOHUnits,LocationActive,LocationTracked";
		//flag = tis.IncrementalTarget(Stage_File, Enrich_File, TargetPOS_Product, TargetPOS_Location, TargetPOS_inventory_schemaString, "Target_INv", Incremental_Folder_Path, Restatement_Folder_Path, "ProductNbr", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
		flag = tis.IncrementalTarget(Stage_File, Enrich_File, TargetPOS_Product, TargetPOS_Location, TargetPOS_inventory_schemaString, "Target_INv", UpdatedFiles_Folder_Path, "ProductNbr", adl_path, sqlContext, sc(), hadoopConf, hdfs, "", "");
	
		hdfs.delete(new org.apache.hadoop.fs.Path(Error_Folder));
		
		assertFalse(flag);
	}

}
