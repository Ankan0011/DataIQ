package com.DataIQ.Test;

import org.apache.log4j.Logger;

public class TestLog {
	final static Logger log = Logger.getLogger(TestLog.class);
	public static void main(String[] args) {
		try {
			int a = 4/0;
		} catch (Exception e) {
			log.error("Error : "+e);
			
		}

	}

}
