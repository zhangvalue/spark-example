package com.learningspark.pairRdd.filter;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.learningspark.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsNotInUsaSolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("filte r").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");
		
		JavaPairRDD<String, String> airportsPairRDD = airportsRDD.mapToPair(getAirportNameAndCountryNamePair());
		JavaPairRDD<String, String> airportsNotInUSA = airportsPairRDD.filter(keyValue -> !keyValue._2.equals("\"United States\""));
		
		airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
		System.out.println("~~~~end~~~~~~");
	}
	
	private static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
		return line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1], line.split(Utils.COMMA_DELIMITER)[3]);
	}

}
