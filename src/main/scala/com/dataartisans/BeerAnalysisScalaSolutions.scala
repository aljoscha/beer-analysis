package com.dataartisans

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

object BeerAnalysisScalaSolutions {

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(1)

    val beers = env.readCsvFile[Beer](
      "/Users/aljoscha/Dev/beer-analysis/beerdata.csv",
      lineDelimiter = "\n",
      fieldDelimiter = '|',
      ignoreFirstLine = true)

    val averageWithCount = beers
      .map { beer => (beer.name, beer.overall, 1) }
      .groupBy("_1")
      .sum("_2").andSum("_3")
      .map { in => (in._1, in._2 / in._3, in._3)}

//    beers.filter( b => b.name.toLowerCase.contains("augustiner") ).distinct("name").print()

    averageWithCount
      .filter(_._1.toLowerCase.contains("augustiner"))
//      .print()

    beers.filter(_.name.toLowerCase.contains("barrel aged"))
      .map { beer => (beer.overall, 1) }
      .sum("_1").andSum("_2")
      .map { in => ("Barrel Aged", in._1 / in._2)}
//      .print()

    beers.filter(!_.name.toLowerCase.contains("barrel aged"))
      .map { beer => (beer.overall, 1) }
      .sum("_1").andSum("_2")
      .map { in => ("Not Barrel Aged", in._1 / in._2)}
//      .print()

    averageWithCount
      .filter(_._3 > 10)
      .map {
      in =>
        in._3 match {
          case count if count < 100 =>
            (in._1, in._2, count, 0)
          case count if count < 1000 =>
            (in._1, in._2, count, 1)
          case count =>
            (in._1, in._2, count, 2)
        }
    }
      .groupBy("_4")
      .sortGroup("_2", Order.ASCENDING)
      .first(20)
      .groupBy("_4")
      .sortGroup("_2", Order.ASCENDING)
      .reduceGroup {
      in =>
        val buffered = in.buffered
        val groupNum = buffered.head._4
        groupNum match {
          case 0 =>
            "< 100:\n" +
              buffered.mkString("\n")
          case 1 =>
            "< 1000:\n" +
              buffered.mkString("\n")
          case 2 =>
            "> 1000:\n" +
              buffered.mkString("\n")

        }
    }
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