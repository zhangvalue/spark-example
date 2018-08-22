package com.learningspark.rdd.persist;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class PersistExample {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> inputIntegers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> integerRDD = sc.parallelize(inputIntegers);
		
		integerRDD.persist(StorageLevel.MEMORY_ONLY());
		
		int result = integerRDD.reduce((x, y) -> x * y);
		System.out.println("reduce: " + result);
		
		long count = integerRDD.count();
		System.out.println(count);
		
		sc.close();
	}

}
