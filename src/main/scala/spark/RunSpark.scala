package spark

import scala.util.Try

object RunSpark extends App {
  val inputFile = Try(args(0)).getOrElse("src/main/resources/covid-data.txt")
  val outputDirectory = Try(args(1)).getOrElse("src/main/resources/output")
  new CovidSparkPerContinentAnalyzer(inputFile, outputDirectory).run()
}
