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


  //Register a user defined function to work on spark DF
  val avgPercChangeUDF = udf((array: Seq[String]) => avgPercChange(array.toArray))
  spark.udf.register("avgPercChangeUDF", avgPercChangeUDF)


  import spark.implicits._

  def Q1Rent(): Unit ={
    /*
    NYC, Los Angeles, Chicago, Houston, Pheonix, Philadelphia, San Antonio, San Diego, Dallas, San Jose
     */

    for(i <- 0 to 5) {

      //Get specific high population density regions
      val columns = DataFrameBuilder.getRent(i).columns
      val regex = "\\w.*-12".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray

      val cityDF = DataFrameBuilder.getRent(i).select(finalColumns.head, finalColumns.tail: _*).
        filter(($"RegionName" === "New York" && $"State" === "NY") ||
          ($"RegionName" === "Los Angeles" && $"State" === "CA") || ($"RegionName" === "Chicago" && $"State" === "IL") ||
          ($"RegionName" === "Houston" && $"State" === "TX") || ($"RegionName" === "Pheonix" && $"State" === "AZ") ||
          ($"RegionName" === "Philadelphia"  && $"State" ==="PA") || ($"RegionName" === "San Antonio" && $"State" === "TX") ||
          ($"RegionName" === "San Diego" && $"State" === "CA") || ($"RegionName" === "Dallas" && $"State" === "TX") ||
          ($"RegionName" === "San Jose" && $"State" === "CA"))

      val df = cityDF.select($"RegionName", $"State", $"Metro", $"CountyName", functions.array(monthColumns.head, monthColumns.tail: _*) as "YearlyPrice")

      if(i == 0){
        println("Studio Rent: ")
      }
      else{
        println(s"$i Bedroom Rent: ")
      }
      df.withColumn("YearlyPrice", avgPercChangeUDF($"YearlyPrice"))
        .withColumnRenamed("YearlyPrice","AvgYearlyPercentChange")
        .orderBy(functions.asc("AvgYearlyPercentChange")).show()
    }

  }

  def Q1Home(): Unit ={
    /*
    NYC, Los Angeles, Chicago, Houston, Pheonix, Philadelphia, San Antonio, San Diego, Dallas, San Jose
     */


    for(i <- 1 to 5) {

      //Get specific high population density regions
      val columns = DataFrameBuilder.getHome(i).columns
      val regex = "\\w.*-01-".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray

      val cityDF = DataFrameBuilder.getHome(i).select(finalColumns.head, finalColumns.tail: _*).
        filter(($"RegionName" === "New York" && $"State" === "NY") ||
          ($"RegionName" === "Los Angeles" && $"State" === "CA") || ($"RegionName" === "Chicago" && $"State" === "IL") ||
          ($"RegionName" === "Houston" && $"State" === "TX") || ($"RegionName" === "Pheonix" && $"State" === "AZ") ||
          ($"RegionName" === "Philadelphia"  && $"State" ==="PA") || ($"RegionName" === "San Antonio" && $"State" === "TX") ||
          ($"RegionName" === "San Diego" && $"State" === "CA") || ($"RegionName" === "Dallas" && $"State" === "TX") ||
          ($"RegionName" === "San Jose" && $"State" === "CA"))

      val df = cityDF.select($"RegionName", $"State", $"Metro", $"CountyName", functions.array(monthColumns.head, monthColumns.tail: _*) as "YearlyPrice")

      println(s"${i} Bedroom Homes: ")
      df.withColumn("YearlyPrice", avgPercChangeUDF($"YearlyPrice"))
        .withColumnRenamed("YearlyPrice","AvgYearlyPercentChange")
        .orderBy(functions.asc("AvgYearlyPercentChange")).show()
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
