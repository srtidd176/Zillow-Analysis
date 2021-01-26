package zillow

import org.apache.spark.sql.SparkSession

object DataFrameBuilder {

  val spark = SparkSession.builder
    .appName("DataFrameBuilder")
    .master("local[4]")
    .getOrCreate()

}
