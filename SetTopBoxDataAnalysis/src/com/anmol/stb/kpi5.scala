package com.anmol.stb
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object kpi5 {
  def main(args: Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage Set Top Box Analysis <Input-File> <Output-File-1> ....")
      System.exit(1)
    }
    
    val spark = SparkSession.builder.appName("KPI4").master("local").getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val filteredData = data.filter(line => {
      val lineSplit = line.split("\\^")
      lineSplit(2).equals("0")
    })
    
    val result = filteredData.map(line => {
      val lineSplit = line.split("\\^")
      val nameValuePairs = XML.loadString(lineSplit(4)) \ "nv"
      val badRecordsPair = nameValuePairs.filter(f => f.\@("n").equals("BadBlocks"))
      if(!badRecordsPair.\@("v").equals("")) badRecordsPair.\@("v").toLong else 0
    } ).sum().toLong
    
    spark.sparkContext.parallelize(Seq("Total Junk Records: %d".format(result))).saveAsTextFile(args(1))
    
    spark.stop
  }  
}