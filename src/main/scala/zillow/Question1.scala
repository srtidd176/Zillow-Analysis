package zillow

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Question1 {
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
    for(i <- 0 to 5) {
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
    }
  }

  def Q1Home(): Unit ={
    /*
    NYC, Los Angeles, Chicago, Houston, Pheonix, Philadelphia, San Antonio, San Diego, Dallas, San Jose
     */
  }

  def change(array: Array[Double]): Double ={
    1.0
  }
}
