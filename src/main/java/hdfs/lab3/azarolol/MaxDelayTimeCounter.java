package hdfs.lab3.azarolol;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class MaxDelayTimeCounter {
    final static String AppName = "MaxDelayTimeCounter";
    final static String SEPARATOR = ",";
    final static int ORIGIN_AIRPORT_ID_INDEX = 11;
    final static int DEST_AIRPORT_ID_INDEX = 14;
    final static int CANCELLED_INDEX = 19;
    final static int DELAY_INDEX = 17;
    final static int AIRPORT_ID_INDEX = 0;
    final static int AIRPORT_NAME_INDEX = 1;

    public static void main (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(AppName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String> flights = sc.textFile("664600583_T_ONTIME_sample.csv");

        JavaPairRDD<Tuple2<String, String>, FlightStat> parsedFlights = parseFlights(flights);
        JavaPairRDD<Tuple2<String, String>, FlightsStat> flightsStat = parsedFlights.aggregateByKey(new FlightsStat(), FlightsStat :: addFlightStat, FlightsStat :: combine);

        JavaPairRDD<String, String> stringAirportDataMap = parseAirports(airports);
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(stringAirportDataMap.collectAsMap());

        JavaRDD<FlightsStat> flightsWithAirport = flightsStat.map(
                s -> new FlightsStat(s, airportsBroadcasted.value())
        );

        flightsWithAirport.saveAsTextFile("output.txt");
    }

    public static JavaPairRDD<Tuple2<String, String>, FlightStat> parseFlights(JavaRDD<String> flights) {
        return flights.mapToPair(
                s -> {
                    String[] flightInformation = s.split(SEPARATOR);
                    String departureAirportID = flightInformation[ORIGIN_AIRPORT_ID_INDEX];
                    String destinationAirportID = flightInformation[DEST_AIRPORT_ID_INDEX];
                    Tuple2<String, String> key = new Tuple2<>(departureAirportID, destinationAirportID);
                    boolean ifCancelled = Objects.equals(flightInformation[CANCELLED_INDEX], "1.00");
                    float delayTime = Float.parseFloat(flightInformation[DELAY_INDEX]);
                    FlightStat value = new FlightStat(ifCancelled, delayTime);
                    return new Tuple2<>(key, value);
                }
        );
    }

    public static JavaPairRDD<String, String> parseAirports(JavaRDD<String> airports) {
        return airports.mapToPair(
                s -> {
                    String[] airportInformation = s.split(SEPARATOR);
                    String airportID = airportInformation[AIRPORT_ID_INDEX];
                    String airportName = airportInformation[AIRPORT_NAME_INDEX];
                    return new Tuple2<>(airportID, airportName);
                }
        );
    }
}