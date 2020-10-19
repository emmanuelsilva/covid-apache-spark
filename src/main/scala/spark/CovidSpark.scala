package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

class CovidSpark(inputCsv: String, outputFolder: String) {

  lazy val spark = SparkSession
    .builder()
    .config("spark.master", "local[4]")
    .appName("CovidAnalyzer")
    .getOrCreate()

  lazy val input_csv_schema = StructType(Seq(
    StructField("iso_code", StringType, false),
    StructField("continent", StringType, false),
    StructField("country", StringType, false),
    StructField("date", DateType, false),
    StructField("new_cases", IntegerType, false),
    StructField("new_deaths", IntegerType, false)
  ))

  def getCovidDataFrame() = {
    spark.
      read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(input_csv_schema)
      .load(this.inputCsv)
      .filter("continent is not null")
  }

  def getStatisticsPerContinentDataFrame() = {
    getCovidDataFrame()
      .groupBy("continent")
      .agg(
        sum("new_cases").as("sum_of_cases"),
        sum("new_deaths").as("sum_of_deaths")
      )
      .withColumn("death rate",
        col("sum_of_deaths")
          .divide(col("sum_of_cases"))
          .multiply(100)
          .cast(DecimalType(4, 2))
      )
      .orderBy(desc("sum_of_deaths"))
  }

  def run(): Unit = {
    getStatisticsPerContinentDataFrame()
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(outputFolder)
  }
}
