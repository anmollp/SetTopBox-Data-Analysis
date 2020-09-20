package com.anmol.stb

import scala.xml.XML
import org.apache.spark.sql.SparkSession

object kpi1 {
  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI-1")
    .master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val split = line.split("\\^")
      split(2).equals("100")
      })
      
    val result_1 = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val xml = XML.loadString(lineSplit(4))
      val deviceId = lineSplit(5)
      val nameValuePairs = xml \ "nv"
      val durationPair = nameValuePairs.apply(f => f.\@("n").equals("Duration"))
      (deviceId, durationPair.\@("v").toLong)
    }).reduceByKey(_ + _).map(row => (row._2, row._1)).sortByKey(false).take(5)
      
    spark.sparkContext.parallelize(result_1.map(row => "Device: %s \t %d hours watched".format(row._2, row._1))).saveAsTextFile(args(1))
    
    
    val result_2 = filteredData.map(line => {
      val xml = XML.loadString(line.split("\\^")(4))
      val nameValuePairs = xml \ "nv"
      val channelPair = nameValuePairs.apply(f => f.\@("n").equals("ChannelType"))
      val durationPair = nameValuePairs.apply(f => f.\@("n").equals("Duration"))
      (channelPair.\@("v"), durationPair.\@("v").toLong)
    }).reduceByKey(_ + _).map(row => (row._2, row._1)).sortByKey(false).take(5)
      
    spark.sparkContext.parallelize(result_2.map(row => "Channel: %s \t %d hours watched".format(row._2, row._1))).saveAsTextFile(args(2))
    
    val result_3 = filteredData.filter(line => {
      val xml = XML.loadString(line.split("\\^")(4))
      val nameValuePairs = xml \ "nv"
      val channelPair = nameValuePairs.apply(f => f.\@("n").equals("ChannelType"))
      channelPair.\@("v").contains("LiveTVMediaChannel")
      }).map(line => {
      val lineSplit = line.split("\\^")
      val deviceId = lineSplit(5)
      deviceId
    }).distinct().count
    
    spark.sparkContext.parallelize(Seq("ChannelType: LiveTVMediaChannel -> %d Devices".format(result_3.toLong))).saveAsTextFile(args(3))
    
    spark.stop
  }
}