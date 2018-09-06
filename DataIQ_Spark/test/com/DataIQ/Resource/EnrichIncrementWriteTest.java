package com.DataIQ.Resource;

import static org.junit.Assert.*;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;
//import com.accenture.spark.testing.JavaDatasetSuiteBase;
import com.accenture.spark.testing.SharedJavaSparkContext;

public class EnrichIncrementWriteTest extends SharedJavaSparkContext {

	EnrichIncrementWrite filewrite = new EnrichIncrementWrite();
	Property Prop = new Property();
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testEnrichIncrementWrite() throws IOException, URISyntaxException {
        //conf().set("spark.driver.allowMultipleContexts", "true");
        //SparkContext sc = new SparkContext(conf());
        SQLContext sqlContext = new SQLContext(sc());
        
        String adl_path = "/DataIQ_Spark";
        
        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);

        //JavaDatasetSuiteBase javaDataSetSuiteBase = new JavaDatasetSuiteBase();
        String FilePath01 = "./TestData/IS_Feed_Incremental.csv";
        String FilePath02 = "./TestData/Incremental/IS_Feed_FirstLoad";
        String Enrich_File_Name = "IS_Feed_FirstLoad.csv";

        Dataset DF_enrich = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("mode", "permissive").load(FilePath01);
        
        Boolean fwrite = filewrite.Enrich_Write(DF_enrich, Enrich_File_Name, FilePath02, adl_path, sqlContext, hadoopConf, hdfs);
        //assertTrue(fwrite);
        assertFalse(fwrite);
	}

}
