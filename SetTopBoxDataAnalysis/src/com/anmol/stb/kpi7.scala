package com.anmol.stb
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi7 {
  def main(args: Array[String]) {
    
    if (args.length < 3) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI7").master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      (lineSplit(2).equals("115") || lineSplit(2).equals("118")) && lineSplit(4).contains("<d>")  
    })
    
    val result_1 = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs = XML.loadString(lineSplit(4)) \ "nv"
      val programPair = nameValuePairs.filter(f => f.\@("n").equals("ProgramId"))
      val durationPair = nameValuePairs.filter(f => f.\@("n").equals("DurationSecs"))
      (programPair.\@("v").toString(), if(!durationPair.\@("v").equals("")) durationPair.\@("v").toLong else 0)
    }).distinct().groupByKey()
    
    spark.sparkContext.parallelize(result_1.map(row => "ProgramId: %s\nDurations(secs):\n%s".format(row._1,row._2.mkString("\n"))).collect().toSeq).saveAsTextFile(args(1))
    
    val result_2 = filteredData.filter(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs = XML.loadString(lineSplit(4)) \ "nv"
      val frequencyPair = nameValuePairs.filter(f => f.\@("n").equals("Frequency"))
      frequencyPair.\@("v").equals("Once")
    }).map(line => {
      val lineSplit = line.split("\\^")
      val deviceId = lineSplit(5)
      deviceId
      }).distinct.count()
      
    spark.sparkContext.parallelize(Seq("# of Devices with frequency - Once : %d".format(result_2.toLong))).saveAsTextFile(args(2))
    
    spark.stop
  }
}