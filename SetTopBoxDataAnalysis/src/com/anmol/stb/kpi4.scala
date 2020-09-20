package com.anmol.stb
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi4 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI4").master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      lineSplit(2).equals("118")
    })
    
    val result_max = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs  = XML.loadString(lineSplit(4)) \ "nv"
      val durationPair = nameValuePairs.filter(f => f.\@("n").equals("DurationSecs"))
      if(!durationPair.\@("v").equals("")) durationPair.\@("v").toLong else Long.MinValue
    })
    
    val result_min = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs  = XML.loadString(lineSplit(4)) \ "nv"
      val durationPair = nameValuePairs.filter(f => f.\@("n").equals("DurationSecs"))
      if(!durationPair.\@("v").equals("")) durationPair.\@("v").toLong else Long.MaxValue
    })
    
    val maxDuration = result_max.max()
    val minDuration = result_min.min()
    
    
    spark.sparkContext.parallelize(Seq("Maximun Duration: " + maxDuration + " ," + "Minimun Duration " + minDuration)).saveAsTextFile(args(1))
    
    spark.stop
  }
}