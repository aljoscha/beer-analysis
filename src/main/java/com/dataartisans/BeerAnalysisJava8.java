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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple3;


public class BeerAnalysisJava8 {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple11<String, Integer, Integer, Float, String,
				Float, Float, Float, Float, Float, Long>> input = env.readCsvFile(
			"/Users/aljoscha/Dev/beer-analysis/beeradvocate.csv.sample")
			.fieldDelimiter('|').lineDelimiter("\n").ignoreFirstLine()
			.types(String.class, Integer.class, Integer.class, Float.class, String.class,
					Float.class, Float.class, Float.class, Float.class, Float.class, Long.class);

		DataSet<Beer> beers = parseInput(input);

		DataSet<Tuple3<String, Float, Long>> averageWithCount = beers
			.map(beer -> new Tuple3<>(beer.name, beer.overall, 1l))
			.groupBy(0)
			.sum(1).andSum(2)
			.map(in -> {
				in.f1 = in.f1 / in.f2;
				return in;
			});
//		averageWithCount.print();


		averageWithCount.filter(in -> in.f0.toLowerCase().contains("augustiner")).print();


		env.execute("Beer Analytics");
	}

	public static DataSet<Beer> parseInput(DataSet<Tuple11<String, Integer, Integer, Float, String,
			Float, Float, Float, Float, Float, Long>> in) {
		return in.map(in1 ->
			new Beer(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in1.f5, in1.f6, in1.f7, in1.f8,
				in1.f9, in1.f10));
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

	public static class Review {
		public Review(float appearance, float aroma, float palate, float taste, float overall,
		              long time, String profileName, String text) {
			this.appearance = appearance;
			this.aroma = aroma;
			this.palate = palate;
			this.taste = taste;
			this.overall = overall;
			this.time = time;
			this.profileName = profileName;
			this.text = text;
		}

		public Review() {}

		public float appearance;
		public float aroma;
		public float palate;
		public float taste;
		public float overall;
		public long time;
		public String profileName;
		public String text;

		@Override
		public String toString() {
			return "Review(" + appearance + "," + aroma + "," + palate + "," + taste + "," +
					overall + "," + time + ")";
		}
	}
}

