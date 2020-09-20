package com.anmol.stb
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi3 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI 3").master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      lineSplit(2).equals("102") || lineSplit(2).equals("113")
    })
    
    val result = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs = XML.loadString(lineSplit(4)) \ "nv"
      val offer = nameValuePairs.apply(p => p.\@("n").equals("OfferId"))
      val price = nameValuePairs.apply(p => p.\@("n").equals("Price"))
      (offer.\@("v"), if(!price.\@("v").equals("")) price.\@("v").toFloat else 0)
    }).distinct().groupByKey().map(record => {
      (record._1, record._2.max)
    })
    
    spark.sparkContext.parallelize(result.map(row => "OfferId: %s  Max Price: %f".format(row._1, row._2)).collect().toSeq).saveAsTextFile(args(1))
    
    spark.stop
  }
}