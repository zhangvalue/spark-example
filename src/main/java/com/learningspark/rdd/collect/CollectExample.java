package com.learningspark.rdd.collect;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectExample {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "casssandra", "hadoop");
		JavaRDD<String> wordRDD = sc.parallelize(inputWords);
		List<String> words = wordRDD.collect();
		
		for (String word : words) {
			System.out.println(word);
		}
		
		sc.close();
	}

}
