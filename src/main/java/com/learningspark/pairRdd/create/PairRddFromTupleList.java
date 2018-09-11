package com.learningspark.pairRdd.create;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRddFromTupleList {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("create").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> tuple = Arrays.asList(new Tuple2<>("Lily", 20),
															  new Tuple2<>("Jack", 29),
															  new Tuple2<>("Mary", 29),
															  new Tuple2<>("James", 8));
		JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuple);
		
		pairRDD.saveAsTextFile("out/pair_rdd_from_tuple_list");
	}

}
