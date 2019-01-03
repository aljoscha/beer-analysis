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
package com.dataartisans;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class BeerAnalysisJava {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple11<String, Integer, Integer, Float, String,
				Float, Float, Float, Float, Float, Long>> input = env.readCsvFile(
			"/Users/aljoscha/Dev/beer-analysis/beerdata.csv")
			.fieldDelimiter('|').lineDelimiter("\n").ignoreFirstLine()
			.types(String.class, Integer.class, Integer.class, Float.class, String.class,
					Float.class, Float.class, Float.class, Float.class, Float.class, Long.class);

		DataSet<Beer> beers = parseInput(input);

//		DataSet<Tuple3<String, Float, Integer>> beerTuples =
				beers.filter( new FilterFunction<Beer>() {

					public boolean filter(Beer beer) throws Exception {
						return beer.style.toLowerCase().contains("pils");
					}

				})
				.map( new MapFunction<Beer, Tuple3<Beer, Float, Integer>>() {

					public Tuple3<Beer, Float, Integer> map(Beer beer) throws Exception {
						return new Tuple3<>(beer, beer.overall, 1);
					}

				})
				.groupBy("f0.name")
				.sum(1).andSum(2)
				.map(new MapFunction<Tuple3<Beer, Float, Integer>, Tuple4<Beer, Float,
						Integer, Integer>>() {

					public Tuple4<Beer, Float, Integer, Integer> map(Tuple3<Beer, Float,
							Integer> in) throws Exception {
						return new Tuple4<>(in.f0, in.f1 / in.f2, in.f2, 1);
					}
				})
				.filter(new FilterFunction<Tuple4<Beer, Float, Integer, Integer>>() {
					@Override
					public boolean filter(Tuple4<Beer, Float, Integer,
							Integer> in) throws Exception {
						return in.f2 > 10;
					}
				})
				.groupBy(3)
				.sortGroup(1, Order.DESCENDING)
				.first(10)
				.print();

		env.execute("Beer Analytics");
	}

	public static DataSet<Beer> parseInput(DataSet<Tuple11<String, Integer, Integer, Float, String,
			Float, Float, Float, Float, Float, Long>> in) {
		return in.map( new MapFunction<Tuple11<String, Integer, Integer, Float, String,
				Float, Float, Float, Float, Float, Long>, Beer>() {
			public Beer map(Tuple11<String, Integer, Integer, Float, String,
					Float, Float, Float, Float, Float, Long> in) throws Exception {

				return new Beer(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, in.f6, in.f7, in.f8,
						in.f9, in.f10);
			}
		});
	}

	public static class Beer {
		public Beer(String name, int beerId, int brewerId, float ABV, String style,
		            float appearance, float aroma, float palate, float taste, float overall,
		            long time) {
			this.name = name;
			this.beerId = beerId;
			this.brewerId = brewerId;
			this.ABV = ABV;
			this.style = style;
			this.appearance = appearance;
			this.aroma = aroma;
			this.palate = palate;
			this.taste = taste;
			this.overall = overall;
			this.time = time;
		}

		public Beer() {}

		public String name;
		public int beerId;
		public int brewerId;
		public float ABV;
		public String style;
		public float appearance;
		public float aroma;
		public float palate;
		public float taste;
		public float overall;
		public long time;

		@Override
		public String toString() {
			return "Beer(" + name + "," + beerId + "," + brewerId + "," + ABV + "," + style + ","
					+ appearance + "," + aroma + "," + palate + "," + taste + "," + overall + ")";
		}
	}
}

