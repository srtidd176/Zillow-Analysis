package zillow

import org.apache.spark.sql.functions.{aggregate, avg, bround, col, collect_list, concat, concat_ws, expr, length, lit, min, monotonically_increasing_id, regexp_extract, regexp_replace}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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

  def TransposeDF(df: DataFrame, Columns: Array[String], Pivot_Col: String): DataFrame = {
    val columnsValue = Columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")

    val unpivotDF = df.select(col(Pivot_Col), expr("stack(" + columnsValue.size + "," + stackCols + ") as (col0,col1)"))
    val pivotDF = unpivotDF.groupBy("col0").pivot(Pivot_Col).agg(concat_ws("", collect_list(col("col1"))))

    pivotDF
  }

  // casting the columns to double for calculation
  def CastDF(column_list:Array[String], df: DataFrame,castedTo:String): DataFrame = {
    var castedDF = df
    for (col_name <- column_list) {
      castedDF = castedDF.withColumn(col_name, df(col_name).cast(castedTo))
    }
    castedDF
  }

  def current_df_columns (df:DataFrame,Not_equals_Column:String): Array[String]={
    val column_list = df.columns.filterNot(_.equals(Not_equals_Column))
    column_list
  }

  def state_renamer (listed_states:Array[String]): Array[String]={
    val states_renamed_list = listed_states.map(state_name => state_name.replace(' ','_'))
    states_renamed_list
  }

  def Q3Rent(i:Int): DataFrame = {

    val df = DataFrameBuilder.getRent(i, "State")

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

    //calling transpose method
    var df3 = TransposeDF(df2, column_names.tail.toArray, "RegionName")

    df3 = df3.select("*") // to get year so average can be taken easily with groupby
      .withColumn("Year", regexp_extract(col("col0"), "([0-9]*[0-9])(_)", 1))
      .drop("col0")

    //renaming each year -> year_Rent
    df3 = df3.withColumn("Year", concat(col("Year"), lit("_Rent")))

    //Get the current state names in the dataset
    val states_List = current_df_columns(df3,"Year")

    //calling cast datatype method
    df3 = CastDF(states_List, df3, "Double")

    //drop any duplicate years
    var df_final = df3.select("Year").distinct()

    //Taking average each state at a time and then joined to new DF
    for (state_name <- states_List) {
      val df_temp = df3.na.fill(0.0)
        .select("Year", state_name)
        .groupBy("Year")
        .agg(bround(avg(state_name), 1).alias(state_name))

      df_final = df_final.join(df_temp, Seq("Year"), "inner")
    }
    // Renaming columns of State to put in underscore for gaps
    val states_renamed = state_renamer(current_df_columns(df_final,"Year")).toBuffer
    states_renamed.prepend("Year")
    val updated_names = states_renamed.toArray

    // transposing the dataframe
    df_final = df_final.toDF(updated_names:_*)
    df_final = TransposeDF(df_final,current_df_columns(df_final,"Year"),"Year")
      .withColumnRenamed("col0","State")

    //current_df_columns(df_final,"State").foreach(println)
    df_final = CastDF(current_df_columns(df_final,"State"),df_final,"Double")

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

    //calling transpose method
    var df3 = TransposeDF(df2, column_names.tail.toArray, "RegionName")

    // to get year so average can be taken easily with groupby
    df3 = df3.select("*")
      .withColumn("Year", regexp_extract(col("col0"), "[2-9][0][1][4-9]", 0))
      .drop("col0")

    //renaming each year column -> year_Home
    df3 = df3.withColumn("Year",concat(col("Year"),lit("_Home")))

    //Get the current state names in the dataset
    val states_List = current_df_columns(df3,"Year")

    //calling cast datatype method
    df3 = CastDF(states_List,df3,"Double")

    //drop any duplicate years
    var df_final = df3.select("Year").distinct()

    //Taking average each state at a time and then joined to new DF
    for (state_name <- states_List) {
      val df_temp = df3.na.fill(0.0)
        .select("Year",state_name)
        .groupBy("Year")
        .agg(bround(avg(state_name),1).alias(state_name))

      df_final = df_final.join(df_temp,Seq("Year"),"inner")
    }
    // Renaming columns of State to put in underscore for gaps
    val states_renamed = state_renamer(current_df_columns(df_final,"Year")).toBuffer
    states_renamed.prepend("Year")
    val updated_names = states_renamed.toArray

    // transposing the dataframe to appear smaller
    df_final = df_final.toDF(updated_names:_*)
    df_final = TransposeDF(df_final,current_df_columns(df_final,"Year"),"Year")
      .withColumnRenamed("col0","State")

    //casting to double since transpose messes up dataypes
    df_final = CastDF(current_df_columns(df_final,"State"),df_final,"Double")

    df_final
  }

  def Q3Income(): DataFrame = {

    var df = DataFrameBuilder.IncomeDF

    // created index col to remove unnecessary rows
    df = df.withColumn("Index", monotonically_increasing_id)
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

    //replacing year with year + income
    var df_final = df
    for (year <- valid_years) {
      df_final = df_final.withColumn(year, regexp_replace(df(year), ",", ""))
        .withColumnRenamed(year, s"${year}_Income")
    }
    //Renaming States then casting to Int for performing calculations
    df_final = df_final.withColumn("State",regexp_replace(df_final("State")," ","_"))
    df_final = CastDF(current_df_columns(df_final,"State"),df_final,"Int")

    df_final
  }

  def Q3Analysis():Unit={
    val yearly_ratio_list = Seq("2019_PI_Ratio", "2018_PI_Ratio",
      "2017_PI_Ratio", "2016_PI_Ratio",
      "2015_PI_Ratio","2014_PI_Ratio")

    val RentDF_names = Seq("Studio","1 bedroom","2 bedrooms",
      "3 bedrooms","4 bedrooms","5 or more bedrooms")

    val HomeDF_names = Seq("1 bedroom","2 bedrooms",
      "3 bedrooms","4 bedrooms","5 or more bedrooms")

    //Creating schema for empty DFs for Least/Most Affordable States
    val schema = new StructType()
      .add("Year",StringType)
      .add("State",StringType)
      .add("PI_ratio",DoubleType)

    //Rent Dataset
    var RentPriceStudioDF = Q3Rent(0)
    var RentPrice1BedDF  = Q3Rent(1)
    var RentPrice2BedDF  = Q3Rent(2)
    var RentPrice3BedDF  = Q3Rent(3)
    var RentPrice4BedDF  = Q3Rent(4)
    var RentPrice5OrMoreBedDF = Q3Rent(5)

    //Home dataset
    var HomePrice1BedDF  = Q3Home(1)
    var HomePrice2BedDF  = Q3Home(2)
    var HomePrice3BedDF  = Q3Home(3)
    var HomePrice4BedDF  = Q3Home(4)
    var HomePrice5OrMoreBedDF  = Q3Home(5)

    var IncomeDF = Q3Income() //Income dataset

    //Joining Rent and Income dataset
    RentPriceStudioDF = RentPriceStudioDF.join(IncomeDF,Seq("State"),"inner")
    RentPrice1BedDF = RentPrice1BedDF.join(IncomeDF, Seq("State"), "inner")
    RentPrice2BedDF = RentPrice2BedDF.join(IncomeDF, Seq("State"), "inner")
    RentPrice3BedDF = RentPrice3BedDF.join(IncomeDF, Seq("State"), "inner")
    RentPrice4BedDF = RentPrice4BedDF.join(IncomeDF, Seq("State"), "inner")
    RentPrice5OrMoreBedDF = RentPrice5OrMoreBedDF.join(IncomeDF, Seq("State"), "inner")

    //Rent + Income DFs list for iteration
    val RentDFs = Seq(RentPriceStudioDF,RentPrice1BedDF,
      RentPrice2BedDF,RentPrice3BedDF,
      RentPrice4BedDF,RentPrice5OrMoreBedDF)

    //Joining Home and Income dataset
    HomePrice1BedDF = HomePrice1BedDF.join(IncomeDF, Seq("State"), "inner")
    HomePrice2BedDF = HomePrice2BedDF.join(IncomeDF, Seq("State"), "inner")
    HomePrice3BedDF = HomePrice3BedDF.join(IncomeDF, Seq("State"), "inner")
    HomePrice4BedDF = HomePrice4BedDF.join(IncomeDF, Seq("State"), "inner")
    HomePrice5OrMoreBedDF = HomePrice5OrMoreBedDF.join(IncomeDF, Seq("State"), "inner")

    //Home + Income DFs list for iteration
    val HomeDFs = Seq(HomePrice1BedDF,HomePrice2BedDF,
    HomePrice3BedDF,HomePrice4BedDF,
    HomePrice5OrMoreBedDF)

    //For printing messages so reader for which dataset are results for
    var Rent_i = 0
    var Home_i = 0

    //Main For Loop for each Rent + Income DF to
    //calculate Price to Income Ratio (PI_ratio)
    for(df_name <- RentDFs){

      val name = RentDF_names(Rent_i)

      val df = df_name
        .withColumn("2019_PI_Ratio", bround((df_name("2019_Rent") / (df_name("2019_Income") / 12)) * 100, 0))
        .withColumn("2018_PI_Ratio", bround((df_name("2018_Rent") / (df_name("2018_Income") / 12)) * 100, 0))
        .withColumn("2017_PI_Ratio", bround((df_name("2017_Rent") / (df_name("2017_Income") / 12)) * 100, 0))
        .withColumn("2016_PI_Ratio", bround((df_name("2016_Rent") / (df_name("2016_Income") / 12)) * 100, 0))
        .withColumn("2015_PI_Ratio", bround((df_name("2015_Rent") / (df_name("2015_Income") / 12)) * 100, 0))
        .withColumn("2014_PI_Ratio", bround((df_name("2014_Rent") / (df_name("2014_Income") / 12)) * 100, 0))

      println(s"Average of Median Rental Price for ${name}")
      df.show()

      var df_Most_Afforable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      var df_Least_Afforable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      for(ratio_year <- yearly_ratio_list) {

        //Most Affordable States Regarding Rent
        var df_temp = df.select("State", ratio_year)
          .withColumn("Year", lit(ratio_year.slice(0, 4)))
          .where(df(ratio_year) =!= 0)
          .withColumnRenamed(ratio_year, "PI_ratio")
          .orderBy("PI_ratio")
          .select("Year", "State", "PI_ratio")
          .limit(1)

        //Least Affordable States Regarding Rent
        var df_temp2 = df.select("State", ratio_year)
          .withColumn("Year", lit(ratio_year.slice(0, 4)))
          .where(df(ratio_year) =!= 0)
          .withColumnRenamed(ratio_year, "PI_ratio")
          .orderBy(col("PI_ratio").desc)
          .select("Year", "State", "PI_ratio")
          .limit(1)

        df_Most_Afforable = df_Most_Afforable.union(df_temp)
        df_Least_Afforable = df_Least_Afforable.union(df_temp2)
      }
      println("Most Affordable state for rent among top 10 populous state by year")
      df_Most_Afforable.show()
      println("Least Affordable state for rent among top 10 populous state by year")
      df_Least_Afforable.show()
      Rent_i += 1
    }

    //Repeated for Home + Income DFs
    for(df_name <- HomeDFs){

      val name = HomeDF_names(Home_i)

      val df = df_name
        .withColumn("2019_PI_Ratio", bround((df_name("2019_Home") / df_name("2019_Income")) * 100, 0))
        .withColumn("2018_PI_Ratio", bround((df_name("2018_Home") / df_name("2018_Income")) * 100, 0))
        .withColumn("2017_PI_Ratio", bround((df_name("2017_Home") / df_name("2017_Income")) * 100, 0))
        .withColumn("2016_PI_Ratio", bround((df_name("2016_Home") / df_name("2016_Income")) * 100, 0))
        .withColumn("2015_PI_Ratio", bround((df_name("2015_Home") / df_name("2015_Income")) * 100, 0))
        .withColumn("2014_PI_Ratio", bround((df_name("2014_Home") / df_name("2014_Income")) * 100, 0))

      println(s"Average of Median House Price for ${name}")
      df.show()

      var df_Most_Afforable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      var df_Least_Afforable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
      for(ratio_year <- yearly_ratio_list){

        var df_temp = df.select("State",ratio_year)
          .withColumn("Year",lit(ratio_year.slice(0,4)))
          .where(df(ratio_year)=!=0)
          .withColumnRenamed(ratio_year,"PI_ratio")
          .orderBy("PI_ratio")
          .select("Year","State","PI_ratio")
          .limit(1)

        var df_temp2 = df.select("State",ratio_year)
          .withColumn("Year",lit(ratio_year.slice(0,4)))
          .where(df(ratio_year)=!=0)
          .withColumnRenamed(ratio_year,"PI_ratio")
          .orderBy(col("PI_ratio").desc)
          .select("Year","State","PI_ratio")
          .limit(1)

        df_Most_Afforable = df_Most_Afforable.union(df_temp)
        df_Least_Afforable = df_Least_Afforable.union(df_temp2)
      }
      println("Most Affordable state for house among top 10 populous state by year")
      df_Most_Afforable.show()
      println("Least Affordable state for house among top 10 populous state by year")
      df_Least_Afforable.show()
      Home_i += 1
    }
  }

}
