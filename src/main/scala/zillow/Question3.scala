package zillow

import org.apache.spark.sql.functions.{avg, bround, col, collect_list, concat_ws, expr, monotonically_increasing_id, regexp_extract, regexp_replace}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.crealytics.spark.excel._

import scala.collection.mutable.ArrayBuffer

object Question3 extends java.io.Serializable {

  val spark = SparkSession.builder()
    .appName("House_Price_Income_Analysis")
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val Populous_States_List: Array[String] = Array("New York", "California", "Texas",
    "Illinois", "Florida", "Ohio",
    "Pennsylvania", "Georgia", "North Carolina", "Michigan")

  def TransposeDF(df: DataFrame, Columns: ArrayBuffer[String], Pivot_Col: String): DataFrame = {
    val columnsValue = Columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")

    val unpivotDF = df.select(col(Pivot_Col), expr("stack(" + columnsValue.size + "," + stackCols + ") as (col0,col1)"))
    val pivotDF = unpivotDF.groupBy("col0").pivot(Pivot_Col).agg(concat_ws("", collect_list(col("col1"))))

    pivotDF
  }

  def CastDF(state_list:Array[String], df: DataFrame,castedTo:String): DataFrame = {
    var castedDF = df
    for (state_name <- state_list) { // casting the columns to double for calculation
      castedDF = df.withColumn(state_name, df(state_name).cast(castedTo))
    }
    castedDF
  }


  def Q3Rent(i:Int): DataFrame = {

      val df = DataFrameBuilder.getRent(i,"State")

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
      df2 = df2.select("*").filter($"RegionName".isin(Populous_States_List: _*))

      var df3 = TransposeDF(df2, column_names.tail, "RegionName") //calling transpose method

      df3 = df3.select("*") // to get year so average can be taken easily by groupby
        .withColumn("Year", regexp_extract(col("Year_Month"), "([0-9]*[0-9])(_)", 1))

      val states_List = df3.columns.slice(1,df3.columns.size-1) //Get the current state names in the dataset

      df3 = CastDF(states_List, df3, "Double") //calling cast datatype method

      var df_final = df3.select("Year").distinct() //drop any duplicate years

      for (state_name <- states_List) {
        val df_temp = df3.na.fill(0.0)
          .select("Year",state_name)
          .groupBy("Year")
          .agg(bround(avg(state_name),1).alias(state_name))

        df_final = df_final.join(df_temp,Seq("Year"),"inner")
      }
      df_final
  }

  def Q3Home(i: Int): DataFrame = {
    val df = DataFrameBuilder.getHome(i, "State")

    //List of columns working with to filter out columns not needed
    //casting valid years values to double
    val column_names: ArrayBuffer[String] = ArrayBuffer("RegionName")
    val regex = "[2-9][0][1][4-9]".r
    val valid_timeline = df.columns
      .filter(p => regex.findFirstIn(p).isDefined).toBuffer

    val valid_timeline_renamed = valid_timeline.map(c => c.replace('-', '_'))
    valid_timeline.prepend("RegionName") // old column names
    column_names ++= valid_timeline_renamed // new column names

    //Renaming columns
    var df2 = df.select(valid_timeline.map(c => col(c)): _*).toDF(column_names: _*)
    df2 = df2.select("*").filter($"RegionName".isin(Populous_States_List: _*))

    //Transpose
    var df3 = TransposeDF(df2, column_names.tail, "RegionName") //calling transpose method

    df3 = df3.select("*") // to get year so average can be taken easily by groupby
      .withColumn("Year", regexp_extract(col("col0"), "[2-9][0][1][4-9]", 0))

    val states_List = df3.columns.slice(1,df3.columns.size-1) //Get the current state names in the dataset

    df3 = CastDF(states_List,df3,"Double") //calling cast datatype method

    var df_final = df3.select("Year").distinct() //drop any duplicate years

    for (state_name <- states_List) {
      val df_temp = df3.na.fill(0.0)
        .select("Year",state_name)
        .groupBy("Year")
        .agg(bround(avg(state_name),1).alias(state_name))

      df_final = df_final.join(df_temp,Seq("Year"),"inner")
    }
    df_final
  }

  def Q3Income(): DataFrame = {

    var df = spark.read.excel(header = false, // Required
      treatEmptyValuesAsNulls = true, // Optional, default: true
      inferSchema = true, // Optional, default: false
      addColorColumns = false, // Optional, default: false
    ).load("/home/sriza/Downloads/h08.xlsx")
      .withColumn("Index", monotonically_increasing_id) // created index col to remove unnecessary rows
      .filter(col("Index") > 3 and col("Index") =!= 5)

    //standard error column removal
    var drop_standard_error_cols = new ArrayBuffer[String]()
    for (i <- 1 to 77) {
      if (i % 2 == 0) {
        var col_name = "_c" + i
        drop_standard_error_cols += col_name
      }
    }
    drop_standard_error_cols += "_c5"
    df = df.drop(drop_standard_error_cols: _*)

    //getting the names of columns from a row and turning it into Array of Strings via RDDs
    val columns_list_tuple = df.limit(1).collect().map(p => p.toString()).toSeq
    val rdd = spark.sparkContext.parallelize(columns_list_tuple)
    val new_columns_list = rdd.flatMap(p => p.split(",")).collect
    val all_years = new_columns_list.slice(1, new_columns_list.size - 1)

    // regex to find valid years working with and appending to new column list
    val regex = "[2][0][1][4-9]".r
    val valid_years = all_years.filter(p => regex.findFirstIn(p).isDefined)
    val valid_columns = ArrayBuffer("State")
    valid_columns ++= valid_years

    //selecting valid columns
    df = df.toDF(new_columns_list: _*)
      .withColumnRenamed("4]", "Index")
      .withColumnRenamed("[State", "State")
      .filter(df("Index") =!= 4)
      .select(valid_columns.map(p => col(p)): _*)

    //Getting the States working with by filtering rows
    df = df.select("*")
      .filter(df("State").isin(Populous_States_List: _*))
      .dropDuplicates("State")

    var df_final = df
    for (year <- valid_years) {
      df_final = df.withColumn(year, regexp_replace(df(year), ",", ""))
        .withColumnRenamed(year, s"${year}_Income")
    }
    df_final
  }

  def Q3_Analysis():Unit={


  }

}
