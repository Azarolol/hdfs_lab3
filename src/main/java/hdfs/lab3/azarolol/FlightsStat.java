package hdfs.lab3.azarolol;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Serializable;
import scala.Tuple2;

import java.util.Map;

public class FlightsStat implements Serializable {
    private String departureAirport;
    private String destinationAirport;
    private int numberOfDelayedFlights;
    private int numberOfCancelledFlights;
    private int maxDelay;
    private int numberOfFlights;

    public FlightsStat() {
        this.numberOfDelayedFlights = 0;
        this.numberOfCancelledFlights = 0;
        this.maxDelay = 0;
        this.numberOfFlights = 0;
    }

    public FlightsStat(int numberOfDelayedFlights, int numberOfCancelledFlights, int maxDelay, int numberOfFlights) {
        this.numberOfDelayedFlights = numberOfDelayedFlights;
        this.numberOfCancelledFlights = numberOfCancelledFlights;
        this.maxDelay = maxDelay;
        this.numberOfFlights = numberOfFlights;
    }

    public FlightsStat (Tuple2<Tuple2<String, String>, FlightsStat> flightsStat, Map<String, String> airportName) {
        FlightsStat flightsStatInfo = flightsStat._2();
        this.numberOfDelayedFlights = flightsStatInfo.getNumberOfDelayedFlights();
        this.numberOfCancelledFlights = flightsStatInfo.getNumberOfCancelledFlights();
        this.maxDelay = flightsStatInfo.getMaxDelay();
        this.numberOfFlights = flightsStatInfo.getNumberOfFlights();

        this.departureAirport = airportName.get(flightsStat._1()._1());
        this.destinationAirport = airportName.get(flightsStat._1()._2());
    }

    public int getMaxDelay() {
        return maxDelay;
    }

    public int getNumberOfCancelledFlights() {
        return numberOfCancelledFlights;
    }

    public int getNumberOfDelayedFlights() {
        return numberOfDelayedFlights;
    }

    public int getNumberOfFlights() {
        return numberOfFlights;
    }

    public static FlightsStat addFlightStat(FlightsStat flightsStat, FlightStat flightStat) {
        flightsStat.numberOfFlights++;
        int flightDelay = flightStat.getDelayTime();
        if (flightDelay > 0) {
            flightsStat.numberOfDelayedFlights++;
            flightsStat.maxDelay = Math.max(flightsStat.maxDelay, flightDelay);
        }
        if (flightStat.isCancelled()) {
            flightsStat.numberOfCancelledFlights++;
        }
        return flightsStat;
    }

    public static FlightsStat combine(FlightsStat first, FlightsStat second) {
        return new FlightsStat(
                first.getNumberOfDelayedFlights() + second.getNumberOfDelayedFlights(),
                first.getNumberOfCancelledFlights() + second.getNumberOfCancelledFlights(),
                Math.max(first.getMaxDelay(), second.getMaxDelay()),
                first.getNumberOfFlights() + second.getNumberOfFlights()
        );
    }

    @Override
    public String toString() {
        String percentageOfCancelledFlights = String.valueOf(this.numberOfCancelledFlights / this.numberOfFlights);
        String percentageOfDelayedFlights = String.valueOf(this.numberOfDelayedFlights / this.numberOfFlights);
        return "From " + destinationAirport + " to " + destinationAirport + ":\n"
                + "Maximal delay time = " + this.maxDelay + "\n"
                + percentageOfDelayedFlights + "% of flights were delayed\n"
                + percentageOfCancelledFlights + "% of flights were cancelled\n";
    }
}
