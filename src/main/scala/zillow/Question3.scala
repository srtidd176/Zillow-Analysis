package zillow

import org.apache.spark.sql.functions.{avg, bround, col, collect_list, concat_ws, expr, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Question3 {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("House_Price_Income_Analysis")
        .master("local[4]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      val df = DataFrameBuilder.homePrice1BedDF

      Q3Rent(df, spark)

    }

    def Q3Rent(df: DataFrame, spark: SparkSession): Unit = {
      import spark.implicits._

      val Populous_City_List: Array[String] = Array("New York", "Los Angeles", "Chicago",
        "Philadelphia", "Phoenix", "Houston",
        "San Antonio", "San Diego", "Dallas",
        "San Jose")

      //List of columns working with to filter out columns not needed
      //casting valid years values to double
      val column_names: ArrayBuffer[String] = ArrayBuffer("RegionName")
      val regex = "[0-9]*[4-9]-[0-9]*".r
      val valid_timeline = df.columns
        .filter(p => regex.findFirstIn(p).isDefined).toBuffer

      val valid_timeline_renamed = valid_timeline.map(c => c.replace('-', '_'))
      valid_timeline.prepend("RegionName") // old column names
      column_names ++= valid_timeline_renamed // new column names

      //Renaming columns
      var df2 = df.select(valid_timeline.map(c => col(c)): _*).toDF(column_names: _*)
      df2 = df2.select("*")
        .filter($"RegionName".isin(Populous_City_List: _*))

      //Transpose
      def TransposeDF(df: DataFrame, Columns: ArrayBuffer[String], Pivot_Col: String): DataFrame = {

        val columnsValue = Columns.map(x => "'" + x + "', " + x)
        val stackCols = columnsValue.mkString(",")

        val unpivotDF = df.select(col(Pivot_Col), expr("stack(" + columnsValue.size + "," + stackCols + ") as (Year_Month,col1)"))
        val pivotDF = unpivotDF.groupBy("Year_Month").pivot(Pivot_Col).agg(concat_ws("", collect_list(col("col1"))))

        pivotDF
      }

      var df3 = TransposeDF(df2, column_names.tail, "RegionName") //calling transpose method

      df3 = df3.select("*") // to get year so average can be taken easily by groupby
        .withColumn("Year", regexp_extract(col("Year_Month"), "([0-9]*[0-9])(_)", 1))


      for (i <- Populous_City_List) { // casting the columns to double for calculation
        df3 = df3.withColumn(i, df3(i).cast("Double"))
      }

      val df_final = df3.na.fill(0.0) //filling any null values as 0.0
        .withColumn("Year", df3("Year").cast("Int")) // to sort the final result
        .select("Year", "New York", "Los Angeles", "Chicago",
          "Philadelphia", "Phoenix", "Houston",
          "San Antonio", "San Diego", "Dallas",
          "San Jose")
        .groupBy("Year")
        .agg(bround(avg("New York"), 1).alias("New York"),
          bround(avg("Chicago"), 1).alias("Chicago"),
          bround(avg("Philadelphia"), 1).alias("Philadelphia"),
          bround(avg("Phoenix"), 1).alias("Phoenix"),
          bround(avg("Dallas"), 1).alias("Dallas"),
          bround(avg("Houston"), 1).alias("Houston"),
          bround(avg("San Antonio"), 1).alias("San Antonio"),
          bround(avg("San Diego"), 1).alias("San Diego"),
          bround(avg("San Jose"), 1).alias("San Jose"),
          bround(avg("Los Angeles"), 1).alias("Los Angeles"))
        .orderBy("Year")

      df_final.show(100)

    }

    def Q3Home(df: DataFrame, spark: SparkSession): Unit = {
    }
}
