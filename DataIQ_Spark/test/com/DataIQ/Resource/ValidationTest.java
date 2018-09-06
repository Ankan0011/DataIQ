package com.DataIQ.Resource;

import static org.junit.Assert.*;

import org.apache.spark.SparkContext;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

import com.accenture.spark.testing.JavaDatasetSuiteBase;
import com.accenture.spark.testing.SharedJavaSparkContext;

public class ValidationTest extends SharedJavaSparkContext {

	Validation valid = new Validation();

	@Test
	public void testCreateDataFrame() {
		conf().set("spark.driver.allowMultipleContexts", "true");
		//SparkContext sc = new SparkContext(conf());
		SQLContext sqlContext = new SQLContext(sc());

		JavaDatasetSuiteBase javaDataFrameSuiteBase = new JavaDatasetSuiteBase();
		String FilePath = "./TestData/DATALAKE_TargetPOS_Sales_20140118_001.csv";

		Dataset DF_Actual = valid.CreateDataFrame(FilePath, sqlContext, ",");
		Dataset DF_expected = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.option("delimiter", ",").option("mode", "permissive").load(FilePath);

		javaDataFrameSuiteBase.assertDatasetEquals(DF_expected, DF_Actual);
	}

	@Test
	public void testCompareColumnLength() {
		Boolean Posative_flag = valid.CompareColumnLength(1, 1);
		assertTrue(Posative_flag);
		Boolean Negative_flag = valid.CompareColumnLength(1, 2);
		assertFalse(Negative_flag);
	}

	@Test
	public void testCompareColumnName() {
		String[] Master_Column = { "Column1", "Column2", "Column3" };
		String[] Data_Column = { "Column1", "Column2", "Column3" };
		scala.collection.immutable.List<String> Posative_flag = valid.CompareColumnName(Master_Column, 3, Data_Column);
		assertTrue(Posative_flag.isEmpty());
	}

	@Test
	public void testCompareColumnSequence() {
		String[] Master_Column = { "Column1", "Column2", "Column3" };
		String[] Data_Column = { "Column1", "Column2", "Column3" };
		scala.collection.immutable.List<String> Posative_flag = valid.CompareColumnSequence(Master_Column, 3,
				Data_Column);
		assertTrue(Posative_flag.isEmpty());
	}

	@Test
	public void testValidateSchema() {
		String[] Master_Column = { "Column1", "Column2", "Column3" };
		String[] Data_Column = { "Column1", "Column2", "Column3" };
		Boolean Posative_flag = valid.ValidateSchema(Master_Column, Data_Column);
		assertTrue(Posative_flag);
	}

}
