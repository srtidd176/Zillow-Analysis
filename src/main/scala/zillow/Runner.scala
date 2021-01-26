package zillow


object Runner extends App {
  println("Hello World")
  DataFrameBuilder.rentPrice1BedDF.explain()
  DataFrameBuilder.rentPrice2BedDF.explain()
  DataFrameBuilder.rentPrice3BedDF.explain()
  DataFrameBuilder.rentPrice4BedDF.explain()
  DataFrameBuilder.rentPrice5OrMoreBedDF.explain()
  DataFrameBuilder.rentPriceStudioDF.explain()

  DataFrameBuilder.homePrice1BedDF.explain()
  DataFrameBuilder.homePrice2BedDF.explain()
  DataFrameBuilder.homePrice3BedDF.explain()
  DataFrameBuilder.homePrice4BedDF.explain()
  DataFrameBuilder.homePrice5OrMoreBedDF.explain()
}
