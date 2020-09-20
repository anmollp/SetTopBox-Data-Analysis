package com.anmol.stb
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi6 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI4").master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      lineSplit(2).equals("107")
    })
    
    val result = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val deviceId = lineSplit(5)
      val nameValuePairs = XML.loadString(lineSplit(4)) \ "nv"
      val buttonPairs = nameValuePairs.filter(f => f.\@("n").equals("ButtonName"))
      (deviceId, buttonPairs.\@("v"))
    }).distinct().groupByKey()
    
    spark.sparkContext.parallelize(result.map(row => "DeviceId: %s\nButtonTypes:\n %s".format(row._1, row._2.mkString("\n"))).collect().toSeq).saveAsTextFile(args(1))
        
    spark.stop
  }
}