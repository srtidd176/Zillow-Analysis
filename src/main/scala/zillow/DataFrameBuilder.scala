package zillow

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameBuilder {

  val spark = SparkSession.builder
    .appName("DataFrameBuilder")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  //Rent
  val rentPrice1BedDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_1Bedroom.csv").toDF()
  val rentPrice2BedDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_2Bedroom.csv").toDF()
  val rentPrice3BedDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_3Bedroom.csv").toDF()
  val rentPrice4BedDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_4Bedroom.csv").toDF()
  val rentPrice5OrMoreBedDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_5BedroomOrMore.csv").toDF()
  val rentPriceStudioDF: DataFrame = spark.read.option("header", true).csv("data/City_MedianRentalPrice_Studio.csv").toDF()

  //Homes
  val homePrice1BedDF: DataFrame = spark.read.option("header", true).csv("data/City_Zhvi_1bedroom.csv").toDF()
  val homePrice2BedDF: DataFrame = spark.read.option("header", true).csv("data/City_Zhvi_2bedroom.csv").toDF()
  val homePrice3BedDF: DataFrame = spark.read.option("header", true).csv("data/City_Zhvi_3bedroom.csv").toDF()
  val homePrice4BedDF: DataFrame = spark.read.option("header", true).csv("data/City_Zhvi_4bedroom.csv").toDF()
  val homePrice5OrMoreBedDF: DataFrame = spark.read.option("header", true).csv("data/City_Zhvi_5BedroomOrMore.csv").toDF()

  def getRent(roomType: Int): DataFrame={
    roomType match{
      case 0 => return rentPriceStudioDF
      case 1 => return rentPrice1BedDF
      case 2 => return rentPrice2BedDF
      case 3 => return rentPrice3BedDF
      case 4 => return rentPrice4BedDF
      case 5 => return rentPrice5OrMoreBedDF
      case _ => null
    }
  }

  def getHome(roomType: Int): DataFrame={
    roomType match{
      case 1 => return homePrice1BedDF
      case 2 => return homePrice2BedDF
      case 3 => return homePrice3BedDF
      case 4 => return homePrice4BedDF
      case 5 => return homePrice5OrMoreBedDF
      case _ => null
    }
  }

}
