package com.learningspark.pairRdd.sort;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortedWordCountSolution {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("SortedWordCountSolution").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> wordRDD = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordPairRDD = wordRDD.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> wordToCountPairs = wordPairRDD.reduceByKey((x, y) -> (x+y));
		
		JavaPairRDD<Integer, String> countToWordPairs = wordToCountPairs.mapToPair(wordToCount -> new Tuple2<>(wordToCount._2(), wordToCount._1()));
		JavaPairRDD<Integer, String> sortedCountToWordPairs = countToWordPairs.sortByKey(false);
		
		JavaPairRDD<String, Integer> sortedWordToCountPairs = sortedCountToWordPairs.mapToPair(
				countToWord -> new Tuple2<>(countToWord._2(), countToWord._1()));
		
		for (Tuple2<String, Integer> wordToCount : sortedWordToCountPairs.collect()) {
			System.out.println(wordToCount._1() + " : " +wordToCount._2());
		}
		
		sc.close();
		System.out.println("~~~~~~~~~~~~~~end~~~~~~~~~~~~~~~~~");;
	}

}
