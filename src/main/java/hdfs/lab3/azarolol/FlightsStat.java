package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightsStat implements Serializable {
    private String departureAirport;
    private String destinationAirport;
    private int numberOfDelayedFlights;
    private int numberOfCancelledFlights;
    private int maxDelay;
    private int numberOfFlights;

    public FlightsStat(String departureAirport, String destinationAirport, int numberOfDelayedFlights, int numberOfCancelledFlights, int maxDelay, int numberOfFlights) {
        this.departureAirport = departureAirport;
        this.destinationAirport = destinationAirport;
        this.numberOfDelayedFlights = numberOfDelayedFlights;
        this.numberOfCancelledFlights = numberOfCancelledFlights;
        this.maxDelay = maxDelay;
        this.numberOfFlights = numberOfFlights;
    }
}
