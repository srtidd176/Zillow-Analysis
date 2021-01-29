package zillow

import org.apache.spark.sql.{SparkSession, functions}
import zillow.Question1.avgPercChangeUDF

import scala.collection.mutable.ArrayBuffer

object Question2 {

  val spark = SparkSession.builder()
    .appName("Question2")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  def Q2Rent(): Unit ={

    for(i <- 2 to 3) {

      //Get specific high population density regions
      val columns = DataFrameBuilder.getRent(i).columns
      val month = "-01"
      val regex = s"\\w.*${month}".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray
      val preFlint = monthColumns.slice(0,monthColumns.indexOf(s"2014${month}")+1)
      preFlint.foreach(println(_))
      println("")
      val postFlint = monthColumns.slice(monthColumns.indexOf(s"2014${month}"), monthColumns.length)
      postFlint.foreach(println(_))
      val cityDF = DataFrameBuilder.getRent(i).select(finalColumns.head, finalColumns.tail: _*).
        filter(($"RegionName" === "Flint" && $"State" === "MI"))
      cityDF.show()

      val df = cityDF.select($"RegionName", $"State", $"Metro", $"CountyName", functions.array(preFlint.head, preFlint.tail: _*) as "PreFlintYearlyPrice",
        functions.array(postFlint.head, postFlint.tail: _*) as "PostFlintYearlyPrice")

      if(i == 0){
        println("Studio Rent: ")
      }
      else{
        println(s"$i Bedroom Rent: ")
      }
      df.withColumn("preFlintYearlyPrice", avgPercChangeUDF($"preFlintYearlyPrice"))
        .withColumnRenamed("preFlintYearlyPrice","preFlintAvgYearlyPercentChange")
        .withColumn("postFlintYearlyPrice", avgPercChangeUDF($"postFlintYearlyPrice"))
        .withColumnRenamed("postFlintYearlyPrice","postFlintAvgYearlyPercentChange").show()
    }
  }

  def Q2Home(): Unit ={

    for(i <- 1 to 5) {

      //Get specific high population density regions
      val columns = DataFrameBuilder.getHome(i).columns
      val month = "-03-31"
      val regex = s"\\w.*${month}".r
      val monthColumns = columns.filter(s => regex.findFirstIn(s).isDefined)
      val finalColumns = ArrayBuffer.concat(Array("RegionName", "State", "Metro",
        "CountyName"), monthColumns).toArray
      val preFlint = monthColumns.slice(0,monthColumns.indexOf(s"2014${month}")+1)
      preFlint.foreach(println(_))
      println("")
      val postFlint = monthColumns.slice(monthColumns.indexOf(s"2014${month}"), monthColumns.length)
      postFlint.foreach(println(_))
      val cityDF = DataFrameBuilder.getHome(i).select(finalColumns.head, finalColumns.tail: _*).
        filter(($"RegionName" === "Flint" && $"State" === "MI"))
      cityDF.show()

      val df = cityDF.select($"RegionName", $"State", $"Metro", $"CountyName", functions.array(preFlint.head, preFlint.tail: _*) as "PreFlintYearlyPrice",
        functions.array(postFlint.head, postFlint.tail: _*) as "PostFlintYearlyPrice")

      if(i == 0){
        println("Studio Rent: ")
      }
      else{
        println(s"$i Bedroom Rent: ")
      }
      df.withColumn("preFlintYearlyPrice", avgPercChangeUDF($"preFlintYearlyPrice"))
        .withColumnRenamed("preFlintYearlyPrice","preFlintAvgYearlyPercentChange")
        .withColumn("postFlintYearlyPrice", avgPercChangeUDF($"postFlintYearlyPrice"))
        .withColumnRenamed("postFlintYearlyPrice","postFlintAvgYearlyPercentChange").show()
    }
  }


}
