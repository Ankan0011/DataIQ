/**
 * This code is used for validation of Raw data(Client data) schema compared to Master Data schema
 * It will validate the column count match between master data and raw data
 * and Find out if any column missing in raw data or any new column with respect to master data
 */

package com.DataIQ.Resource

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.commons.lang.StringUtils

class Validation {

  val logg: LogWrite = new LogWrite()
  val Prop: Property = new Property()
  //val adl_path = Prop.getProperty("adl_path")
  val FolderPath_temp = Prop.getProperty("Raw_To_Stage_LogInfo")

  def CreateDataFrame(FilePath: String, sqlContext: SQLContext, delimiter: String): DataFrame =
    {
      /**
       * This  function will create Data Frame of the file in the FilePath
       * Input Parameter :- FilePath:String, sqlContext:SQLContext
       * Return :- DF:DataFrame
       */

      val FileContent_Temp = "Parameters |FilePath: "+FilePath+ "|delimiter: "+delimiter
   
      val DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("delimiter", delimiter).option("mode", "permissive").load(FilePath)
      return DF
    }
  def CompareColumnLength(MasterColumn_Length: Int, RawDataColumn_Length: Int): Boolean =
    {
      /**
       * This function will compare the length of Master data column and Raw data column
       * Input Parameter :- MasterColumn_Length:Int,RawDataColumn_Length:Int
       * Return :- NA
       */
      var message: String = ""
      var flag: Boolean = false
      if (MasterColumn_Length == RawDataColumn_Length) {
        message = "Column Count of Raw Data match the Master Data"
        flag = true
      } else {
        flag = false
        if (MasterColumn_Length > RawDataColumn_Length) {
          message = "Column Count of Raw Data LESS than Master Data by: " + (MasterColumn_Length - RawDataColumn_Length)
        } else {
          message = "Column Count of Raw Data MORE than Master Data by: " + (RawDataColumn_Length - MasterColumn_Length)
        }
      }
      val FileContent_Temp = "Parameters |MasterColumn_Length: " +MasterColumn_Length +"|RawDataColumn_Length: "+RawDataColumn_Length +"Returns |flag: "+flag
      return flag
    }
  def CompareColumnName(Column1: Array[String], Column1Length: Int, Column2: Array[String]): List[String] =
    {
      /**
       * This function will Find the column missing in Column2 with respect to Column1
       * Input Parameter :- Column1:Array[String], Column1Length:Int, Column2:Array[String]
       * Return :- List[String]
       */
    
      var Missing_Column = new ListBuffer[String]()

      for (i <- 0 to (Column1Length - 1)) {
        val Temp_Column = Column1.apply(i)
        if (!Column2.contains(Temp_Column)) {
          Missing_Column += Temp_Column
        }
      }
      val FileContent_Temp = "Parameters |Column1: "+Column1.mkString(",")+"|Column1Length: "+Column1Length+"|Column2: "+Column2.mkString(",")+"Returns |Missing_Column: "+Missing_Column.mkString(",")
      return Missing_Column.toList

    }
  def CompareColumnSequence(Column1: Array[String], Column1Length: Int, Column2: Array[String]): List[String] =
    {
      /**
       * This function will compare the sequence of column2 with that of column1
       * Input Parameter :- Column1:Array[String], Column1Length:Int, Column2:Array[String]
       * Return :- UnSequenced_Column:List[String]
       */
      var UnSequenced_Column = new ListBuffer[String]()

      for (i <- 0 to (Column1Length - 1)) {
        val Temp_Column1 = Column1.apply(i)
        val Temp_Column2 = Column2.apply(i)
        if (!Temp_Column1.equals(Temp_Column2)) {
          UnSequenced_Column += Temp_Column1
        }
      }
      
      val FileContent_Temp = "Parameters |Column1: "+Column1.mkString(",")+"|Column1Length: "+Column1Length+"|Column2: "+Column2.mkString(",")+"Returns |UnSequenced_Column: "+UnSequenced_Column.mkString(",")
      return UnSequenced_Column.toList
     
    }
  def ValidateSchema(Master_Column: Array[String], RawData_Column: Array[String]): Boolean =
    {
      var valid: Boolean = false

      val timestamp = current_timestamp()

      try {

        val MasterColumn_Length = Master_Column.length //Length of Master dataframe column
        val RawDataColumn_Length = RawData_Column.length //Length of Raw Data dataframe column

        /**
         * Compare the length of Master data columns and Raw Data columns
         */
        val ColumnCountFlag = CompareColumnLength(MasterColumn_Length, RawDataColumn_Length)

        /**
         * Find out the column name that are present in master but not in raw data
         */
        val Missing_Master_Column = CompareColumnName(Master_Column, MasterColumn_Length, RawData_Column) //this list contain column name that are in master but not in raw data

        /**
         * Find out the column name that are present in Raw data but not in master data
         */
        val New_RawData_Column = CompareColumnName(RawData_Column, RawDataColumn_Length, Master_Column) //this list contain column name that are present in Raw data but not in master data

        /**
         * Find if the column sequence in raw data is same as master data
         */
        val UnsequenceColumn = CompareColumnSequence(Master_Column, MasterColumn_Length, RawData_Column)

        /**
         * The following if condition will check if all the flags are true
         */
        if (ColumnCountFlag && Missing_Master_Column.isEmpty && New_RawData_Column.isEmpty && UnsequenceColumn.isEmpty) {
          valid = true
        } else {
          valid = false

        }

      } catch {
        case t: Throwable => {
          t.printStackTrace()
          valid = false
        }
      }

      val FileContent_Temp = "Parameters |Master_Column: "+Master_Column.mkString(",")+"|RawData_Column: "+RawData_Column.mkString(",")+"Returns |valid: "+valid
      return valid
    }
}