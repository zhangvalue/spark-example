package com.learningspark.pairRdd.mapValues;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.learningspark.rdd.commons.Utils;

import scala.Tuple2;

public class AirportsUppercaseSolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("mapValues").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");
		JavaPairRDD<String, String> airportsPairRDD = airportsRDD.mapToPair(getAirportNameAndCountryName());
		JavaPairRDD<String, String> upperCase = airportsPairRDD.mapValues(countryName -> countryName.toUpperCase());
		
		upperCase.saveAsTextFile("out/airports_uppercase.text");
		System.out.println("~~~~~~~~~~~end~~~~~~~~~~~~");
		sc.close();
	}
	
	private static PairFunction<String, String, String> getAirportNameAndCountryName() {
		return line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1], line.split(Utils.COMMA_DELIMITER)[3]);
	}
}
