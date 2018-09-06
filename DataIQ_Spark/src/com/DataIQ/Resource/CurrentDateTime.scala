package com.DataIQ.Resource

import java.util.Date
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.sql.Timestamp

class CurrentDateTime extends java.io.Serializable {

  def PresentDate(): String =
    {
      val d: Timestamp = new Timestamp(System.currentTimeMillis())
      val formattter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS")
      val transfer_format: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val TimeStamp = formattter.parse(d.toString())
      val day = transfer_format.format(TimeStamp)
      return day
    }
  def day(t1: String, Format: String): String =
    {
      /**
       * This method will convert the date format
       * input format is 'yyyy-MM-dd' and output format is 'MM/dd/yyyy'
       * input parameter:- t1: String
       * output parameter:- day:String
       */
      var day = ""
      try {
        val format: SimpleDateFormat = new SimpleDateFormat(Format) //Input format
        val formatter: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy") //Output format
        val d = format.parse(t1)
        day = formatter.format(d)

      } catch {
        case t: Throwable => { //In case of exception 
          if (t.toString().length() > 0) {
            day = "Unparceable"
            return day
          }
        }
      }
      return day.toString()
    }
  def CurrentDate(): String =
    {
      /**
       * This method will give the current timestamp
       * input:- Unit
       * output:- Timestamp:String
       */
      var d: Timestamp = new Timestamp(System.currentTimeMillis())

      return d.getTime.toString()
    }
  def CurrentTime(): String =
    {
      /**
       * This method will give the current timestamp
       * input:- Unit
       * output:- Timestamp:String
       */
      var d: Timestamp = new Timestamp(System.currentTimeMillis())

      return d.toString()
    }
  def To_Milli(date: String, format_temp: String): Long =
    {
      var res_op = 0L
      try {
        val format: SimpleDateFormat = new SimpleDateFormat(format_temp)
        res_op = format.parse(date).getTime
      } catch {
        case t: Throwable => t.printStackTrace() // TODO: handle error
      }
      return res_op
    }
  
}