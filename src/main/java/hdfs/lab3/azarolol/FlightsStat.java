package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightsStat implements Serializable {
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
}
