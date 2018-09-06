package com.DataIQ.Resource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration

class RawToStageError extends java.io.Serializable {

  def ErrorHandle(fileName: String, Status: String, Description: String, RowCount: String, StartTime: String, EndTime: String, ErrorPath: String, adl_path: String, sc: SparkContext, sqlContext: SQLContext, hdfs: FileSystem, hadoopConf: Configuration): Boolean =
    {
      var Flag = false
      try {
        val CT: CurrentDateTime = new CurrentDateTime()
        val Date = CT.PresentDate()
        val Current_Timestamp = CT.CurrentDate()

        //val ErrorFolder = ErrorPath+"/"+Date
        val ErrorFolder = ErrorPath.replaceAll("date", Date)

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(ErrorFolder))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(ErrorFolder))
        }

        val Table_Schema = StructType(Array(StructField("FileName", StringType, true)))
        val newRow = sc.parallelize(Seq(fileName)).map(t => Row(t))
        val DF = sqlContext.createDataFrame(newRow, Table_Schema)
        val ResDF = DF.withColumn("Status", lit(Status)).withColumn("Error_description", lit(Description)).withColumn("Total_row_Count", lit(RowCount)).withColumn("StartTime", lit(StartTime)).withColumn("EndTime", lit(EndTime))

        ResDF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(ErrorFolder)

        val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/part*.csv"))
        var path_04 = listStatusReName(0).getPath()
        Flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(ErrorFolder.replaceAll(adl_path, "") + "/" + fileName.replaceAll(".csv", "") + "_" + Current_Timestamp + ".csv"))
      } catch {
        case t: Throwable =>
          {
            Flag = false
          }
      }
      return Flag

    }
}
