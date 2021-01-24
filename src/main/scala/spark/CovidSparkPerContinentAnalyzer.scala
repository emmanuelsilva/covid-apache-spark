package spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

class CovidSparkPerContinentAnalyzer(inputCsv: String, outputDirectory: String) {

  val spark = SparkSession
    .builder()
    .config("spark.master", "local[*]")
    .appName("CovidAnalyzer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def extract() = {
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(this.inputCsv)
  }

  def aggregateToHighestNumberOfDeathsPerMonth(raw: DataFrame) = {
    raw
      .filter("continent is not null")                                                       //clear to get valid data only
      .select("continent", "location", "date", "new_cases", "new_deaths", "new_vaccinations") //select only desired columns
      .withColumn("month_date", trunc(col("date"), "Month"))                   //compute new column
      .groupBy("continent", "month_date")                                                    //nice grouping functions
      .agg(
        sum(nvl(col("new_cases"), 0)).as("sum_of_cases"),                     //nvl as a custom function
        sum(nvl(col("new_deaths"), 0)).as("sum_of_deaths"),
        sum(nvl(col("new_vaccinations"), 0)).as("sum_of_vaccinations")
      )
      .withColumn("death_rate",                                                                //compute function using column values
        nvl(
          col("sum_of_deaths")
            .divide(col("sum_of_cases"))
            .multiply(100)
            .cast(DecimalType(4, 2))
          ,
          0
        )
      )
      .withColumn("index",                                                                     //Window functions
        row_number()
          .over(Window
            .partitionBy("month_date")
            .orderBy(desc("sum_of_deaths"))
        )
      )
      .orderBy(col("month_date"), desc("sum_of_deaths"))                          //nice grouping function
      //.filter("index == 1")                                                                          //filter again to show only valid data
  }

  /**
   * Create a syntax sugar function for NVL
   */
  def nvl(col: Column, replaceValue: Any) = {
    when(col.isNull, lit(replaceValue)).otherwise(col)
  }

  def run(): Unit = {
    extract()                                              //E
      .transform(aggregateToHighestNumberOfDeathsPerMonth) //T
      .write                                               //L
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .partitionBy("continent")                  //Save CSV partitioned by continent
      .save(outputDirectory)
  }
}
