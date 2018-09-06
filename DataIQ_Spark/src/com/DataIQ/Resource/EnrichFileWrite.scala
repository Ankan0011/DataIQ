package com.DataIQ.Resource

import org.apache.spark.sql.DataFrame
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class EnrichFileWrite {
  def Enrich_Write(Enrich_File: String, Combined_Clean_DF: DataFrame, Enrich_DF: DataFrame, adl_path: String, Enrich_File_Name: String, hadoopConf: Configuration, hdfs: FileSystem): Boolean =
    {
      var flag = false
      try {
        Combined_Clean_DF.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(Enrich_File)
        
        //Delete the existing file in Enrich Folder
        if (!Enrich_DF.limit(1).rdd.isEmpty) {
          val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
          var path_02 = listStatus(0).getPath()
          hdfs.delete(path_02)
        }

        //The following line will rename the enrich file
        val listStatusReName = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/part*.csv"))
        var path_04 = listStatusReName(0).getPath()
        flag = hdfs.rename(path_04, new org.apache.hadoop.fs.Path(Enrich_File.replaceAll(adl_path, "") + "/" + Enrich_File_Name))
        
      } catch {
        case t: Throwable => {
          t.printStackTrace()
          flag = false
        }
      }

      return flag
    }
}