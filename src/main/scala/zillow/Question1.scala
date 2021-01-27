package zillow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer

object Question1 extends java.io.Serializable{
  val spark = SparkSession.builder()
    .appName("Q1")
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

 import spark.implicits._

  def Q1Rent(): Unit ={
    /*
    NYC, Los Angeles, Chicago, Houston, Pheonix, Philadelphia, San Antonio, San Diego, Dallas, San Jose
     */
    for(i <- 0 to 0) {

      //Get specific high population density regions
      val columns = DataFrameBuilder.getRent(i).columns
      val regex = "\\w.*-12".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray

      val cityDF = DataFrameBuilder.getRent(i).select(finalColumns.head, finalColumns.tail: _*).
        filter($"RegionName" === "New York" ||
          $"RegionName" === "Los Angeles" || $"RegionName" === "Chicago" ||
          $"RegionName" === "Houston" || $"RegionName" === "Pheonix" ||
          $"RegionName" === "Philadelphia" || $"RegionName" === "San Antonio" ||
          $"RegionName" === "San Diego" || $"RegionName" === "Dallas" ||
          $"RegionName" === "San Jose")
      cityDF.show()

      //Group price columns in an array
      val cityRDD = cityDF.rdd
      val newRDD: RDD[Row] = cityRDD.map(row => Row(row.get(0),row.get(1),row.get(2),row.get(3),
        Array(row.get(4), row.get(5), row.get(6), row.get(7), row.get(8), row.get(9), row.get(10)
        , row.get(11), row.get(12), row.get(13))))

      val schema = new StructType()
        .add(StructField("RegionName", StringType, true))
        .add(StructField("State", StringType, true))
        .add(StructField("Metro", StringType, true))
        .add(StructField("CountyName", StringType, true))
        .add(StructField("YearlyPrice", ArrayType(StringType, true), true))

      val df = spark.createDataFrame(newRDD, schema)

      //Register a user defined function to work on spark DF
      val avgPercChangeUDF = udf((array: Seq[String]) => avgPercChange(array.toArray))
      spark.udf.register("avgPercChangeUDF", avgPercChangeUDF)

      df.withColumn("YearlyPrice", avgPercChangeUDF($"YearlyPrice"))
        .withColumnRenamed("YearlyPrice","AvgYearlyPercentChange")
        .orderBy(functions.asc("AvgYearlyPercentChange"))show()
    }


  }

  def Q1Home(): Unit ={
    /*
    NYC, Los Angeles, Chicago, Houston, Pheonix, Philadelphia, San Antonio, San Diego, Dallas, San Jose
     */

    for(i <- 1 to 5) {
      val columns = DataFrameBuilder.getHome(i).columns
      val regex = "\\w.*-12".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray

      val cityDF = DataFrameBuilder.getHome(i).select(finalColumns.head, finalColumns.tail: _*).
        filter($"RegionName" === "New York" ||
          $"RegionName" === "Los Angeles" || $"RegionName" === "Chicago" ||
          $"RegionName" === "Houston" || $"RegionName" === "Pheonix" ||
          $"RegionName" === "Philadelphia" || $"RegionName" === "San Antonio" ||
          $"RegionName" === "San Diego" || $"RegionName" === "Dallas" ||
          $"RegionName" === "San Jose")
      cityDF.show()

    }
  }

  def avgPercChange(array: Array[String]): Double ={
    var total_change = 0.0
    var count = 0.0
    var prev: Double = 0.0
    for(price <- array){
      if(price != null){
        if(prev == 0.0){
          prev = price.toDouble
        }
        else{
          total_change += (price.toDouble - prev)/prev * 100
          count += 1
          prev = price.toDouble
        }
      }
    }
    total_change/count

  }
}
