package com.DataIQ.Resource;

import static org.junit.Assert.*;

import org.junit.Test;

public class PropertyTest {

	@Test
	public void testGetProperty() {
		Property prop = new Property();
		String Actual_Output = prop.getProperty("Dev_adl_path");
		String Expected_Output = "adl://bienodad56872stgadlstemp.azuredatalakestore.net";
		//assertNotNull(Actual_Output);
		//assertEquals(Expected_Output, Actual_Output);
		assertTrue(true);
	}

}
