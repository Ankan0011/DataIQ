package com.DataIQ.Resource;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;
import com.accenture.spark.testing.JavaDatasetSuiteBase;
import com.accenture.spark.testing.SharedJavaSparkContext;
import com.DataIQ.Resource.StageToEnrichErrorCalculate;

public class StageToEnrichErrorCalculateTest extends SharedJavaSparkContext {

	StageToEnrichErrorCalculate obj = new StageToEnrichErrorCalculate();

	@Test
	public void testCalculateError() throws Throwable {

		//conf().set("spark.driver.allowMultipleContexts", "true");
		//SparkContext sc = new SparkContext(conf());
		SQLContext sqlContext = new SQLContext(sc());

		String adl_path = "/DataIQ_Spark";
		Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);

		String flag = "harmonic";
		String Error_Col = "DATE,SDESC";
		String FileName = "NielsenPeriod.csv";
		String Enrich_DF_url = "./TestData/Harmonic_ErrorDF.csv";
		String Error_Record_url = "./TestData/Error_Record_NielsenPeriod.csv";

		Dataset Harmonic_ErrorDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.option("delimiter", ",").option("mode", "permissive").load(Enrich_DF_url);


		JavaDatasetSuiteBase javaDataFrameSuiteBase = new JavaDatasetSuiteBase();
		Dataset Actual_result_01 = obj.CalculateError(FileName, Error_Col, Harmonic_ErrorDF, sqlContext, adl_path,
				hadoopConf, hdfs, "harmonic");
		Dataset Actual_result_02 = obj.CalculateError(FileName, Error_Col, Harmonic_ErrorDF, sqlContext, adl_path,
				hadoopConf, hdfs, "harmonic");
		javaDataFrameSuiteBase.assertDatasetEquals(Actual_result_01, Actual_result_01);

		String fileName = "WalmartPOS_HairOil";
		String ErrorPath = "./TestData/Random";
		String Enrich_DF_url01 = "./TestData/NielsenPeriod_20170720_1500550030528.csv";

		Dataset ResDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.option("delimiter", ",").option("mode", "permissive").load(Enrich_DF_url01);

		Boolean Actual_result = obj.ErrorHandle(fileName, ErrorPath, ResDF, adl_path, sc(), sqlContext, hdfs, hadoopConf);
		//assertTrue(Actual_result);
		assertFalse(Actual_result);
		hdfs.delete(new org.apache.hadoop.fs.Path(ErrorPath));

	}

	
}
