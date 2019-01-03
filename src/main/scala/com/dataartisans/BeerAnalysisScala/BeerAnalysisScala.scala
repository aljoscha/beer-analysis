package com.dataartisans

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

object BeerAnalysisScala {

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(1)

    val beers = env.readCsvFile[Beer2](
      "/Users/aljoscha/Dev/beer-analysis/beerdata.csv",
      lineDelimiter = "\n",
      fieldDelimiter = '|',
      ignoreFirstLine = true)

    beers
      .filter { in => in.name.toLowerCase.contains("augustiner")}
      .distinct("name")
      .print()


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