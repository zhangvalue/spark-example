package com.learningspark.rdd.sumofprimenumber;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfPrimeNumbers {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("sumofprimenumber").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
		JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
		
		JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());
		JavaRDD<Integer> intNumbers = validNumbers.map(number -> Integer.valueOf(number));
		
		System.out.println("Sum is: " + intNumbers.reduce((x, y) -> x + y));
		
		sc.close();
	}

}
