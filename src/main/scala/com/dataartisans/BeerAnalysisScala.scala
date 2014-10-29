package com.dataartisans

import org.apache.flink.api.scala._

object BeerAnalysisScala {

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(1)

    val beers = env.readCsvFile[Beer](
      "/home/flink/beer-analysis/beerdata.csv.sample",
      lineDelimiter = "\n",
      fieldDelimiter = '|',
      ignoreFirstLine = true)

    val averageWithCount = beers
      .map { beer => (beer.name, beer.overall, 1) }
      .groupBy("_1")
      .sum("_2").andSum("_3")
      .map { in => (in._1, in._2 / in._3, in._3)}


    // execute program
    env.execute("Beer Analysis")
  }


}

case class Beer(
    name: String,
    beerId: Int,
    brewerId: Int,
    ABV: Float,
    style: String,
    appearance: Float,
    aroma: Float,
    palate: Float,
    taste: Float,
    overall: Float,
    time: Long)