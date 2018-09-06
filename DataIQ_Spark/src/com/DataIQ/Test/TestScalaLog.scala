package com.DataIQ.Test

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import java.io.BufferedOutputStream
import com.DataIQ.Resource.Property

object TestScalaLog {
  
  def main(args: Array[String]): Unit = {

    val Prop: Property = new Property()
    
    val spark_serializer = Prop.getProperty("spark_serializer")
    println(spark_serializer)
    
  }
}