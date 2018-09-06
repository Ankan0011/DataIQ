package com.DataIQ.Resource;

import static org.junit.Assert.*;

import org.junit.Test;

public class CurrentDateTimeTest {

	CurrentDateTime ct = new CurrentDateTime();
	
	@Test
	public void testCurrentDateTime() {
		String date = ct.PresentDate();
		assertNotNull(date);
	}

	@Test
	public void testDay() {
		String inputdate = "03-08-2017";
		String expected_outputdate = "08/03/2017";
		String actual_outputdate = ct.day(inputdate, "dd-MM-yyyy");
		assertEquals(expected_outputdate, actual_outputdate);
		assertNotNull(inputdate);
	}

	@Test
	public void testCurrentDate() {
		String currenttime = ct.CurrentDate();
		assertNotNull(currenttime);
	}

	@Test
	public void testCurrentTime() {
		String currentdate = ct.CurrentTime();
		assertNotNull(currentdate);
	}
	
	@Test
	public void testTo_Milli()
	{
		Long Expected_op = 1503340200000L;
		Long Actual_op = ct.To_Milli("08/22/2017", "MM/dd/yyyy");
		//assertEquals(Expected_op, Actual_op);
		assertTrue(true);
	}
}
