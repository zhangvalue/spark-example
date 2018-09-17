package com.learningspark.pairRdd.aggregation.reducebykey;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> wordRDD = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordPairRDD = wordRDD.mapToPair(word -> new Tuple2<>(word, 1));

		JavaPairRDD<String, Integer> wordCounts = wordPairRDD.reduceByKey((x, y) -> (x + y));

		Map<String, Integer> wordCountsMap = wordCounts.collectAsMap();
		for (Entry<String, Integer> wordCountPair : wordCountsMap.entrySet()) {
			System.out.println(wordCountPair.getKey() + " : " + wordCountPair.getValue());
		}
		
		sc.close();
	}

}
