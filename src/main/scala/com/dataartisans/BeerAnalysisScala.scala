package com.dataartisans

import org.apache.flink.api.common.operators.Order
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
      .sortGroup("_2", Order.DESCENDING)
      .first(20)
      .groupBy("_4")
      .sortGroup("_2", Order.DESCENDING)
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
//      .print()

    averageWithCount
      .filter(_._1.toLowerCase.contains("augustiner"))
      .print()

//    val barrelAged = parsedInput.filter(_.name.toLowerCase.contains("barrel aged"))
//      .map { beer => (beer.review.overall, 1) }
//      .sum("_1").andSum("_2")
//      .map { in => ("Barrel Aged", in._1 / in._2)}
//
//    val nonBarrelAged = parsedInput.filter(!_.name.toLowerCase.contains("barrel aged"))
//      .map { beer => (beer.review.overall, 1) }
//      .sum("_1").andSum("_2")
//      .map { in => ("Not Barrel Aged", in._1 / in._2)}

//    barrelAged.print()
//    nonBarrelAged.print()

//    val becks = parsedInput.filter( b => b.name.toLowerCase.contains("becks") ).distinct("name")
//    becks.print()

//    val miller = parsedInput.filter( b => b.name.toLowerCase.contains("miller") ).distinct("name")
//    val bud = parsedInput.filter( b => b.name.toLowerCase.contains("bud") ).distinct("name")
//    val pbr = parsedInput.filter( b => b.name.toLowerCase.contains("pabst") ).distinct("name")
//
//    miller.print()
//    bud.print()
//    pbr.print()


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