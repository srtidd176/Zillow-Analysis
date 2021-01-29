package zillow

import org.apache.spark.sql.functions.{avg, bround, col, collect_list, concat_ws, expr, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Question3 extends java.io.Serializable {

      val spark = SparkSession.builder()
        .appName("House_Price_Income_Analysis")
        .master("local[4]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._

    def Q3Rent(): Unit = {

      for (i <- 0 to 5) {

        var Populous_City_List: Array[String] = Array("Phoenix","New York","Chicago", "San Jose",
          "Dallas","Los Angeles", "Philadelphia",
          "Houston", "San Antonio", "San Diego"
        )

        if(i==5){
          Populous_City_List = Populous_City_List.slice(5,10)
        }

        if(i==0){
          Populous_City_List = Populous_City_List.slice(1,10)
        }

        val df = DataFrameBuilder.getRent(i)

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

        if(i==5) {
          val df_final = df3.na.fill(0.0) //filling any null values as 0.0
            .withColumn("Year", df3("Year").cast("Int")) // to sort the final result
            .select("Year", Populous_City_List: _*)
            .groupBy("Year")
            .agg(bround(avg("Philadelphia"), 1).alias("Philadelphia"),
              bround(avg("Houston"), 1).alias("Houston"),
              bround(avg("San Antonio"), 1).alias("San Antonio"),
              bround(avg("San Diego"), 1).alias("San Diego"),
              bround(avg("Los Angeles"), 1).alias("Los Angeles"))
            .orderBy("Year")

          println(s"Rent ${i}BedRoom")
          df_final.show()
        }

        else if(i==0){
          val df_final = df3.na.fill(0.0) //filling any null values as 0.0
            .withColumn("Year", df3("Year").cast("Int")) // to sort the final result
            .select("Year", Populous_City_List: _*)
            .groupBy("Year")
            .agg(bround(avg("New York"), 1).alias("New York"),
              bround(avg("Chicago"), 1).alias("Chicago"),
              bround(avg("Philadelphia"), 1).alias("Philadelphia"),
              bround(avg("Dallas"), 1).alias("Dallas"),
              bround(avg("Houston"), 1).alias("Houston"),
              bround(avg("San Antonio"), 1).alias("San Antonio"),
              bround(avg("San Diego"), 1).alias("San Diego"),
              bround(avg("San Jose"), 1).alias("San Jose"),
              bround(avg("Los Angeles"), 1).alias("Los Angeles"))
            .orderBy("Year")

          println("Annual Average Median Rent for Studio")
          df_final.show()
        }

        else {
          val df_final = df3.na.fill(0.0) //filling any null values as 0.0
            .withColumn("Year", df3("Year").cast("Int")) // to sort the final result
            .select("Year", Populous_City_List: _*)
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

          println(s"Annual Average Median Rent for ${i}BedRoom")
          df_final.show()
        }

      }
    }

    def Q3Home(df: DataFrame, spark: SparkSession): Unit = {
    }
}
