package com.learningspark.rdd.nasaApacheWebLogs;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Sample {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("sample").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> datas = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> dataRDD = sc.parallelize(datas);
		
		JavaRDD<Integer> sampleRDD = dataRDD.sample(false, 0.5, System.currentTimeMillis());
		System.out.println("===================sampleRDD=========1=========");
		sampleRDD.foreach(v -> System.out.println(v));
		
		JavaRDD<Integer> sampleRDD2 = dataRDD.sample(true, 0.5, System.currentTimeMillis());
		System.out.println("============sampleRDD=========2=============");
		sampleRDD2.foreach(v -> System.out.println(v));

		sc.close();
	}

}
