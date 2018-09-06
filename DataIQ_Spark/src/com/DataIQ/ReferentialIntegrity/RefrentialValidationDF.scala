package com.DataIQ.ReferentialIntegrity

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, LongType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat 
import java.util.Date
import org.apache.spark.sql.Column

class RefrentialValidationDF {
  def CompareDataframe(DataFileDF: DataFrame, MasterFileDF: DataFrame, DataFileCol: String, MasterFileCol: String, sc: SparkContext, sqlContext: SQLContext): DataFrame =
    {
      var ReturnDF = sqlContext.createDataFrame(sc.emptyRDD[Row], DataFileDF.schema)
      try {
        import sqlContext.implicits._

        val DF1 = DataFileDF.as("D1").join(MasterFileDF.as("D2"), DataFileDF(DataFileCol) === MasterFileDF(MasterFileCol)).select($"D1.*")

        ReturnDF = ReturnDF.unionAll(DF1)

      } catch {
        case t: Throwable => {
          t.printStackTrace()

        } // TODO: handle error
      }

      return ReturnDF
    }
    
  
  }