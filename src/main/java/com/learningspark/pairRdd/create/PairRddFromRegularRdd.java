package com.learningspark.pairRdd.create;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class PairRddFromRegularRdd {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("create").setMaster("local[*]");
		
		JavaSparkContext sContext = new JavaSparkContext(conf);
		
		List<String> inputStrings = Arrays.asList("Lily 23", "Jack 29", "Mary 29", "James 8");
		
		JavaRDD<String> regularRDD = sContext.parallelize(inputStrings);
		
		JavaPairRDD<String, Integer> pairRDD = regularRDD.mapToPair(getPairFunction());
		
		pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd");
	}
	
	
	private static PairFunction<String, String, Integer> getPairFunction() {
		return s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
	}
}
