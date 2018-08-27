package com.learningspark.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SameHostsSolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
		JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
		
		JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);
		JavaRDD<String> augustFistHosts = augustFirstLogs.map(line -> line.split("\t")[0]);
		
		JavaRDD<String> intersection = julyFirstHosts.intersection(augustFistHosts);
		
		JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));
		
		cleanedHostIntersection.saveAsTextFile("out/nasa_logs_same_hosts.csv");
		
		sc.close();
	}

}
