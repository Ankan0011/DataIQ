package com.DataIQ.ReferentialIntegrity

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, LongType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class RefrentialValidationErrorDF 
{
  def CompareErrorDataframe(DataFileDF: DataFrame, MasterFileDF: DataFrame, DataFileCol: String, MasterFileCol: String, sc: SparkContext, sqlContext: SQLContext): DataFrame =
    {
      import sqlContext.implicits._
      var Error_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], DataFileDF.schema)
      try {
        val DF1 = DataFileDF.as("D1").join(MasterFileDF.as("D2"), DataFileDF(DataFileCol) === MasterFileDF(MasterFileCol)).select($"D1.*")

        val UnmatchedDataFile_DF = DataFileDF.except(DF1)

        Error_DF = (Error_DF.unionAll(UnmatchedDataFile_DF))

      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }

      return Error_DF
    }
  }