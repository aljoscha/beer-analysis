/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object Converter {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setDegreeOfParallelism(1)

    val input = env.readCsvFile[Tuple1[String]](
      "/Users/aljoscha/Dev/beer-analysis/beeradvocate.txt.sample",
      lineDelimiter = "\n\n",
      fieldDelimiter = 0)

    val parsedInput = parseInput(input)

    val csv = parsedInput map { in =>
      s"${in.name}|${in.beerId}|${in.brewerId}|${in.ABV}|${in.style}|" +
        s"${in.review.appearance}|${in.review.aroma}|${in.review.palate}|${in.review.taste}|" +
        s"${in.review.overall}|${in.review.time}"
    }

    csv.writeAsText("/Users/aljoscha/Dev/beer-analysis/beeradvocate.csv.sample",
      writeMode = WriteMode.OVERWRITE)
    // execute program
    env.execute("Converter")
  }

  def parseInput(input: DataSet[Tuple1[String]]): DataSet[ConvBeer] =
    input.map {
      in =>
        var name = ""
        var beerId = -1
        var brewerId = -1
        var ABV = -1f
        var style = ""
        var appearance = -1f
        var aroma = -1f
        var palate = -1f
        var taste = -1f
        var overall = -1f
        var time = -1L
        var profileName = ""
        var text = ""

        in._1.split('\n') foreach { str =>
          try {
            str match {
              case r"beer/name: (.*)$v" => name = v
              case r"beer/beerId: (.*)$v" => beerId = v.toInt
              case r"beer/brewerId: (.*)$v" => brewerId = v.toInt
              case r"beer/ABV: (.*)$v" => ABV = v.toFloat
              case r"beer/style: (.*)$v" => style = v
              case r"review/appearance: (.*)$v" => appearance = v.toFloat
              case r"review/aroma: (.*)$v" => aroma = v.toFloat
              case r"review/palate: (.*)$v" => palate = v.toFloat
              case r"review/taste: (.*)$v" => taste = v.toFloat
              case r"review/overall: (.*)$v" => overall = v.toFloat
              case r"review/time: (.*)$v" => time = v.toLong
              case r"review/profileName: (.*)$v" => profileName = v
              case r"review/text: (.*)$v" => text = v
              case _ => // Ignore
            }
          } catch {
            case e: NumberFormatException => // ignore
          }
        }

        ConvBeer(name, beerId, brewerId, ABV, style,
          new ConvReview(appearance, aroma, palate, taste, overall, time, profileName, text))
    }
}

case class ConvBeer(
  name: String,
  beerId: Int,
  brewerId: Int,
  ABV: Float,
  style: String,
  review: ConvReview)

case class ConvReview(
  appearance: Float,
  aroma: Float,
  palate: Float,
  taste: Float,
  overall: Float,
  time: Long,
  profileName: String,
  text: String)
