package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightStat implements Serializable {
    private String departureAirport;
    private String destinationAirport;
    private boolean ifCancelled;
    private int delayTime;

    public FlightStat(String departureAirport, String destinationAirport, boolean ifCancelled, int delayTime) {
        this.departureAirport = departureAirport;
        this.destinationAirport = destinationAirport;
        this.delayTime = delayTime;
        this.ifCancelled = ifCancelled;
    }
}
