package com.anmol.stb

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi2 {
  def main(args: Array[String]) {
    
     if (args.length < 2) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
     
    val spark = SparkSession.builder.appName("KPI-2")
    .master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      lineSplit(2).equals("101")
    })
    
    val result = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val deviceId = lineSplit(5)
      val xml = XML.loadString(lineSplit(4))
      val nameValuePairs = xml \ "nv"
      val powerState = nameValuePairs.filter(f => f.\@("n").equals("PowerState"))
      if (powerState.\@("v").equals("ON") || powerState.\@("v").equals("OFF")) {
        deviceId
      }
    }).distinct().count()
    
    spark.sparkContext.parallelize(Seq("# of devices with PowerState ON/OFF:\t %d".format(result))).saveAsTextFile(args(1))
    spark.stop
  }
}