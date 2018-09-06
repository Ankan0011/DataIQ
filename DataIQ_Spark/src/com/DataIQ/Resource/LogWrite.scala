package com.DataIQ.Resource

import org.apache.hadoop.fs.Path
import java.io.BufferedOutputStream

class LogWrite {
  def InsertLog(adl_path: String, FolderPath_temp: String,FileName:String, FileContent_Temp: String): Unit =
    {
      var os: BufferedOutputStream = null
      val CT:CurrentDateTime = new CurrentDateTime()
      val timestamp  = CT.CurrentDate()
      val presentDate = CT.PresentDate()
      val PresentDateTime = CT.CurrentTime()
      val FileContent = PresentDateTime+" - "+FileContent_Temp
      
      val FolderPath = FolderPath_temp+"/"+presentDate
      val FilePath = FolderPath+"/"+FileName+"_"+timestamp+".txt"
      try {
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(adl_path), hadoopConf)
        
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(FolderPath))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(FolderPath))
        }
        
        val output = hdfs.create(new Path(FilePath))
        os = new BufferedOutputStream(output)
        os.write(FileContent.getBytes())

      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      } finally {
        try {
          os.close()
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
        }
        
      }
    }
}