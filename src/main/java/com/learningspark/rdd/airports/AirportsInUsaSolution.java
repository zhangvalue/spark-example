package com.learningspark.rdd.airports;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.learningspark.rdd.commons.Utils;


public class AirportsInUsaSolution {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> airports = sc.textFile("in/airports.text");
		
		JavaRDD<String> airportsInUSA = airports.filter(Line -> Line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
		
		JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(Line -> {
			String[] splits = Line.split(Utils.COMMA_DELIMITER);
			return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
		});
		
		airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");
		sc.close();
	}

}
