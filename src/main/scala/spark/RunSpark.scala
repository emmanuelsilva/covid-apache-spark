package spark

import scala.util.Try

object RunSpark extends App {
  val inputFile = Try(args(0)).getOrElse("src/main/resources/covid-data.csv")
  val outputFolder = Try(args(1)).getOrElse("src/main/resources/output")

  new CovidSpark(inputFile, outputFolder).run()
}
