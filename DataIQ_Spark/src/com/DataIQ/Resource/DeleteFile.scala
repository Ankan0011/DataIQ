package com.DataIQ.Resource

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration

class DeleteFile {
  def Delete_File(hdfs: FileSystem, filePath: String): Boolean =
    {
      var flag = false
      try {
        flag = hdfs.delete(new org.apache.hadoop.fs.Path(filePath))
      } catch {
        case t: Throwable => {
          t.printStackTrace()
          flag = false
        }
      }
      return flag
    }
  def DeleteMultipleFile(adl_path: String, FilePath: String, hadoopConf: Configuration, hdfs: FileSystem): Boolean =
    {
      var flag = false
      try {
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(FilePath.replaceAll(adl_path, "") + "/*"))
        if (!listStatus.isEmpty) {
          for (i <- 0 to (listStatus.length - 1)) {
            val Raw_File_Ind = listStatus.apply(i).getPath.toString()
            flag = hdfs.delete(new org.apache.hadoop.fs.Path(Raw_File_Ind))
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }

      return flag
    }
}