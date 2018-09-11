package com.learningspark.pairRdd.groupbykey;


import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.learningspark.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsByCountrySolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");
		JavaPairRDD<String, String> countryAndAirportNamePairRDD =
				airportsRDD.mapToPair(line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[3], line.split(Utils.COMMA_DELIMITER)[1]));
		JavaPairRDD<String, Iterable<String>> airportByCountry= countryAndAirportNamePairRDD.groupByKey();
		
		for(Entry<String, Iterable<String>> airports : airportByCountry.collectAsMap().entrySet()) {
			System.out.println(airports.getKey() + " : " + airports.getValue());
		}
		
		sc.close();
	}

}
