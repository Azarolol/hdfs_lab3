package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightStat implements Serializable {
    private boolean ifCancelled;
    private int delayTime;

    public FlightStat(boolean ifCancelled, int delayTime) {
        this.delayTime = delayTime;
        this.ifCancelled = ifCancelled;
    }
}
