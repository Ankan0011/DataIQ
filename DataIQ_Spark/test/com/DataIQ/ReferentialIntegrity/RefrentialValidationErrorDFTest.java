package com.DataIQ.ReferentialIntegrity;

import static org.junit.Assert.*;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

import com.accenture.spark.testing.JavaDatasetSuiteBase;
import com.accenture.spark.testing.SharedJavaSparkContext;

public class RefrentialValidationErrorDFTest extends SharedJavaSparkContext{

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCompareErrorDataframe() {
		
		RefrentialValidationErrorDF rdf = new RefrentialValidationErrorDF();
		JavaDatasetSuiteBase javaDataFrameSuiteBase = new JavaDatasetSuiteBase();
		conf().set("spark.driver.allowMultipleContexts", "true");
		//SparkContext sc = new SparkContext(conf());
		SQLContext sqlContext = new SQLContext(sc());
		String DataFile = "./TestData/TGT_CDT_ICECREAM.csv";
		String MasterFile = "./TestData/TGT_PRODUCT_MSTR.csv";
		Dataset DataFileDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("mode", "permissive").load(DataFile);
		Dataset MasterDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("mode", "permissive").load(MasterFile);
		
		Dataset Actual_Result1 = rdf.CompareErrorDataframe(DataFileDF, MasterDF, "ProductNbr", "ProductNbr", sc(), sqlContext);
		Dataset Actual_Result2 = rdf.CompareErrorDataframe(DataFileDF, MasterDF, "ProductNbr", "ProductNbr", sc(), sqlContext);
		
		javaDataFrameSuiteBase.assertDatasetEquals(Actual_Result1, Actual_Result2);
	}

}
