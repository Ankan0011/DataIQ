package com.DataIQ.Resource

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import java.net.URI

class EnrichIncrementWrite {
  def Enrich_Write(DF: DataFrame, FileName: String, FolderPath: String, adl_path: String, sqlContext: SQLContext, hadoopConf: Configuration, hdfs: FileSystem): Boolean =
    {
      var flag = false
      try {

        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(FolderPath))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(FolderPath))
         
        }
        val destinationFile = FolderPath

        DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(destinationFile)
        val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/part*.csv"))
        var path_04 = listStatusReName(0).getPath()
        flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(destinationFile.replaceAll(adl_path, "") + "/" + FileName))
      } catch {
        case t: Throwable => {
          t.printStackTrace()
          flag = false
        }
      }

      return flag
    }
}