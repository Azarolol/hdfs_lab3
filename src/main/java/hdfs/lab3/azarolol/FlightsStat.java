package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightsStat implements Serializable {
    private int numberOfDelayedFlights;
    private int numberOfCancelledFlights;
    private int maxDelay;
    private int numberOfFlights;

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

    public static void addFlightStat(FlightStat flightStat) {
        this.numberOfFlights++;
        int flightDelay = flightStat.getDelayTime();
        if (flightDelay > 0) {
            this.numberOfDelayedFlights++;
            this.maxDelay = Math.max(this.maxDelay, flightDelay);
        }
        if (flightStat.isCancelled()) {
            this.numberOfCancelledFlights++;
        }
    }

    public FlightsStat combine(FlightsStat first, FlightsStat second) {
        return new FlightsStat(
                first.getNumberOfDelayedFlights() + second.getNumberOfDelayedFlights(),
                first.getNumberOfCancelledFlights() + second.getNumberOfCancelledFlights(),
                Math.max(first.getMaxDelay(), second.getMaxDelay()),
                first.getNumberOfFlights() + second.getNumberOfFlights()
        );
    }
}
