package com.DataIQ.StageToEnrichProcess

import org.joda.time.DateTime
import org.joda.time.Period
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeConstants
import java.text.SimpleDateFormat
import java.util.Calendar
import java.net.URI
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import com.DataIQ.Resource.StageToEnrichErrorCalculate
import com.DataIQ.Resource.CurrentDateTime
import com.DataIQ.Resource.LogWrite

class MissingWeekData extends java.io.Serializable {
  def convertDateFunc(x: String): String = {
    var resDate = ""
    try {
      val formatter = DateTimeFormat.forPattern("MM/dd/yyyy")
      val formatter1 = DateTimeFormat.forPattern("yyyy/")
      val from_temp = formatter.parseDateTime(x)
      val from = from_temp.plusDays(1)
      resDate = from.getYear + "/" + from.getWeekOfWeekyear
    } catch {
      case t: Throwable => { resDate = "Unparcable" }
    }

    return resDate
  }
  // DateRange function
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] = Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

  def POSMissingWeeks(Enrich_File: String, col_name: String, adl_path: String, DestinationFile: String, hadoopConf: Configuration, hdfs: FileSystem, sqlContext: SQLContext, sc: SparkContext, FolderPath_temp: String): Unit =

    {
      import sqlContext.implicits._
      val CT: CurrentDateTime = new CurrentDateTime()
      val logg: LogWrite = new LogWrite()
      var sb: StringBuffer = new StringBuffer(CT.CurrentTime() + "  :  Inside method: MissingWeekData.POSMissingWeeks \n")
      val Enrich_File_Name = Enrich_File.split("/").last.replaceAll(".csv", "")

      var flag = false
      val StartTime = CT.CurrentTime()
      var EndTime = ""
      var resError_Count = ""
      val Error_Stage_File = Enrich_File.replaceAll("/Enriched", "").replaceAll(adl_path, "")
      try {

        //Creation of folder for destination, if doesn't exists
        if (!hdfs.isDirectory(new org.apache.hadoop.fs.Path(DestinationFile))) {
          hdfs.mkdirs(new org.apache.hadoop.fs.Path(DestinationFile))
        }

        sb.append(CT.CurrentTime() + " : Enrich dataframe reading started.... \n")
        //Create dataframes from POS Enriched dataset
        val Enrich_DF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("escape", "\"").option("mode", "permissive").option("inferSchema", "false").load(Enrich_File).select(col_name)
        val Enrich_DF_Count = Enrich_DF.count().toString()

        sb.append(CT.CurrentTime() + " : Enrich dataframe reading completed. \n")
        val Col_Enrich_rdd = Enrich_DF.rdd.map(x => Row(x.get(0), convertDateFunc(x.get(0).toString())))

        val temp_Schema = StructType(Array(StructField(col_name, StringType, true), StructField("Unique_Week_Number", StringType, true)))
        val Col_Enrich_DF = sqlContext.createDataFrame(Col_Enrich_rdd, temp_Schema)

        //Get the dataset path
        val Enrich_File_Path = Enrich_File.replaceAll(adl_path, "")

        sb.append(CT.CurrentTime() + " : Calculate 'from' (Start period_key) date started... \n")
        //Calculate "from" (Start period_key) date
        val from_temp = Col_Enrich_DF.orderBy(unix_timestamp(col(col_name), "MM/dd/yyyy")).take(1).apply(0).apply(0).toString()
        val formatter = DateTimeFormat.forPattern("MM/dd/yyyy");
        val from_initial = formatter.parseDateTime(from_temp);
        val from = from_initial.withDayOfWeek(6)

        sb.append(CT.CurrentTime() + " : Calculate 'from' (Start period_key) date completed. \n")
        val end_temp = Col_Enrich_DF.orderBy(unix_timestamp(col(col_name), "MM/dd/yyyy").desc).take(1).apply(0).apply(0).toString()

        val end_initial = formatter.parseDateTime(end_temp);

        /*val end_temp = Col_Enrich_DF.agg(max(unix_timestamp(col(col_name), "MM/dd/yyyy"))).take(1).apply(0).apply(0).toString()
      val end_initial = formatter.parseDateTime(end_temp);*/

        sb.append(CT.CurrentTime() + " : Calculate 'to' (Start period_key) date started... \n")
        //Calculate "to" (Current period_key) date
        val to_temp = new DateTime
        val offset = ((to_temp.getDayOfWeek - DateTimeConstants.SATURDAY) + 7) % 7
        val to = to_temp.minusDays(offset)

        sb.append(CT.CurrentTime() + " : Calculate 'to' (Start period_key) date completed. \n")
        sb.append(CT.CurrentTime() + " : Create Date range of weekends by passing from and to date to dateRange() function. \n")
        //Create Date range of weekends by passing from and to date to dateRange() function
        val range = dateRange(from, to, new Period().withDays(7))
        val datesrange = range.toList

        // Convert list of daterange into required period_key format
        val daterdd = datesrange.map { x => x.toString("MM/dd/yyyy") }

        // Create rdd by parallizing datarange list into array[Row]      
        val rdd1 = sc.parallelize(daterdd).map(x => Row(x, convertDateFunc(x.toString())))
        sb.append(CT.CurrentTime() + " : Create dataframe with range of weekends. \n")

        //Create dataframe with range of weekends           
        val week_rddSchema = StructType(Array(StructField(col_name, StringType, true), StructField("Week_Number", StringType, true)))

        val week_DF_temp1 = sqlContext.createDataFrame(rdd1, week_rddSchema)

        //Get file name of POS dataset
        val DatasetName = Enrich_File.split("/").last

        sb.append(CT.CurrentTime() + " : get distinct period_keys and Add availability column. \n")
        //get distinct period_keys and Add availability column      

        val dataDF_temp = Col_Enrich_DF.select("Unique_Week_Number").distinct()
        val dataDF = dataDF_temp.withColumn("Availability", lit("Yes"))
        //Join dataDF dataframe with week_DF_temp1 dataframe based on period_key

        //val joinedDF = week_DF_temp1.join(dataDF, Seq(col_name), joinType = "outer")
        val joinedDF = week_DF_temp1.join(dataDF, week_DF_temp1("Week_Number") === dataDF("Unique_Week_Number"), "left_outer").select(week_DF_temp1(col_name), dataDF("Availability"))

        // Add Dataset_Name column

        val newDF = joinedDF.sort(col_name).withColumn("Dataset_Name", lit(Enrich_File_Path))

        sb.append(CT.CurrentTime() + " : Check availability and make No for un available weekends. \n")
        //Check availability and make No for un available weekends

        val outputDF = newDF.withColumn("Availability", expr("case when Availability is null then 'No' else 'Yes' end")).toDF()

        //Add DBInsert date
        sb.append(CT.CurrentTime() + " : Add DBInsert date. \n")
        val format = new SimpleDateFormat("MM/dd/yyyy")
        val InsertDate = format.format(Calendar.getInstance().getTime())

        val Initial_Date = from_initial.getMonthOfYear + "/" + from_initial.getDayOfMonth + "/" + from_initial.getYear
        val last_Date = end_initial.getMonthOfYear + "/" + end_initial.getDayOfMonth + "/" + end_initial.getYear

        val OutputDF_Final = outputDF.withColumn("DBInsertDate", lit(InsertDate)).withColumn("MinDate", lit(Initial_Date)).withColumn("MaxDate", lit(last_Date)).select("Dataset_Name", col_name, "Availability", "DBInsertDate", "MinDate", "MaxDate")
        val OutputDF_Final_count = OutputDF_Final.count().toString()
        //val OutputDF_Final = outputDF.withColumn("DBInsertDate", lit(InsertDate)).withColumn("MinDate", lit(from_initial.toString())).withColumn("MaxDate", lit(end_initial.toString())).select("Dataset_Name", col_name, "Availability", "DBInsertDate","MinDate","MaxDate") 

        sb.append(CT.CurrentTime() + " : Write the file in provided location. \n")
        //Write the file in provided location 
        OutputDF_Final.repartition(1).write.option("header", "true").option("escape", "\"").mode("append").csv(DestinationFile)

        // Insert into SQL DB

        //Rename part file
        val listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(DestinationFile.replaceAll(adl_path, "") + "/part*.csv")) // '/child/adl/path(current name)' => is the file path like: '/Enriched/Test/NielsenMrkt/*.csv'
        var path_05 = listStatus(0).getPath() //get the path, this is of path type and not string type
        flag = hdfs.rename(path_05, new org.apache.hadoop.fs.Path(DestinationFile + "/" + DatasetName))
        sb.append(CT.CurrentTime() + " : File rename is done for the file written in provided location. \n")
        EndTime = CT.CurrentTime()
        if (flag) {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Processed", Enrich_DF_Count, resError_Count, OutputDF_Final_count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "MissingWeekData.POSMissingWeeks - "+"Processed")
          sb.append(CT.CurrentTime() + " : MissingWeekData.POSMissingWeeks - Processed \n")
        } else {
          val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
          ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Rejected", Enrich_DF_Count, resError_Count, OutputDF_Final_count, StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
          //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "MissingWeekData.POSMissingWeeks - "+"Rejected")
          sb.append(CT.CurrentTime() + " : MissingWeekData.POSMissingWeeks - Rejected \n")
        }
      } catch {
        case t: Throwable =>
          {
            t.printStackTrace()
            EndTime = CT.CurrentTime()
            val ErrorRecords: StageToEnrichErrorCalculate = new StageToEnrichErrorCalculate()
            ErrorRecords.CalculateRecord(Enrich_File_Name, Error_Stage_File, "Exception Occured while Processing", "", "", "", StartTime, EndTime, sc, sqlContext, adl_path, hadoopConf, hdfs)
            //logg.InsertLog(adl_path, FolderPath_temp, Enrich_File_Name.replaceAll(".csv", ""), "MissingWeekData.POSMissingWeeks - "+t.getMessage)
            sb.append(CT.CurrentTime() + " : Exception occured: " + t.getMessage + " \n")
          }
      } finally {
        sb.append(CT.CurrentTime() + "  : POSMissingWeeks method execution completed. \n")
        logg.InsertLog(adl_path, FolderPath_temp, Enrich_File.replaceAll(".csv", ""), sb.toString())
      }

    }
}
