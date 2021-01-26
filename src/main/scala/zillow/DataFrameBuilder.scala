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
}
