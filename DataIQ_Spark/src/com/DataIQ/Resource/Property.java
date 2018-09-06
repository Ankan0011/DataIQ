/**
 * This code is used to access the 'configuration.properties' file
 * This file is placed under :- src/main/java
 * The file contain key and value pair, we get the value by giving key
 */
package com.DataIQ.Resource;

import java.io.InputStream;
import java.util.Properties;

public class Property 
{
	public String getProperty(String name) 
	{
		/**
		 * This method is used to access the values in property files
		 * input parameter:- name:String   this is the key name
		 * return:- content:String         this id the value
		 */
		Properties prop = new Properties();
		InputStream input = null;
		String content = null;
		String fileName = "configuration.properties";   //The name of property file
		try
		{
			input = Property.class.getClassLoader().getResourceAsStream(fileName);
			prop.load(input);
			if(input == null)
			{
				System.out.println("Property file not found");
			}
			
			content = prop.getProperty(name);          //to read from prop file
			
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally 
		{
			return content;
		}
		
	}
	
}
