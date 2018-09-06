package com.DataIQ.Resource;

import static org.junit.Assert.*;

import org.junit.Test;

import com.accenture.spark.testing.SharedJavaSparkContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class RawToStageErrorTest extends SharedJavaSparkContext {

	@Test
	public void testErrorHandle() throws IOException, URISyntaxException {

		//conf().set("spark.driver.allowMultipleContexts", "true");
		//SparkContext sc = new SparkContext(conf());
		SQLContext sqlContext = new SQLContext(sc());
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		String adl_path = "/DataIQ_Spark";
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);

		RawToStageError object = new RawToStageError();
		String fileName = "WalmartPOS_HairOil";
		String Status = "Processed";
		String Description = "NA";
		String RowCount = "200";
		String StartTime = "12:00 AM";
		String EndTime = "01:00 AM";
		String ErrorPath = "./TestData/Random";

		Boolean Actual_Result = object.ErrorHandle(fileName, Status, Description, RowCount, StartTime, EndTime,
				ErrorPath, adl_path, sc(), sqlContext, hdfs, hadoopConf);
		//assertTrue(Actual_Result);
		assertFalse(Actual_Result);
		hdfs.delete(new org.apache.hadoop.fs.Path(ErrorPath));
		
	}

}
