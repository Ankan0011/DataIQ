package com.DataIQ.Resource;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

public class DeleteFileTest {
	
	DeleteFile dfile = new DeleteFile();
	
	Property Prop = new Property();
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testDeleteMultipleFile() throws IOException, URISyntaxException {
        
        String adl_path = "/DataIQ_Spark";
	    	      
        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf);
        
        String FilePath = "./TestData/DeleteFile";
        
        Boolean DF_Actual = dfile.DeleteMultipleFile(adl_path, FilePath, hadoopConf, hdfs);

        assertFalse(DF_Actual);
	}
}
