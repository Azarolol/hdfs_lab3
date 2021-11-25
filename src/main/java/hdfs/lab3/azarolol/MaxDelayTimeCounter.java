package hdfs.lab3.azarolol;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MaxDelayTimeCounter {
    final static String AppName = "MaxDelayTimeCounter";
    final static String SEPARATOR = ",";

    public static void main (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(AppName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String> flights = sc.textFile("664600583_T_ONTIME_sample.csv");


    }

    public JavaPairRDD<Tuple2<String, String>, FlightsStat> parseFlights(JavaRDD<String> flights) {
        return flights.mapToPair(
                s -> {
                    String[] flightsInformation = s.split(SEPARATOR);
                    String departureAirportId = 
                }
        )
    }
}