package com.learningspark.rdd.count;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountExample {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
		JavaRDD<String> wordRDD = sc.parallelize(inputWords);
		
		System.out.println("Count: " + wordRDD.count());
		Map<String, Long> wordCountByValue = wordRDD.countByValue();
		
		System.out.println("CountByValue:");
		
		for (Map.Entry<String, Long> entry : wordCountByValue.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		
		sc.close();
	}

}
