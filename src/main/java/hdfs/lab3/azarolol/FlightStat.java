package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightStat implements Serializable {
    private boolean cancelled;
    private int delayTime;

    public FlightStat(boolean ifCancelled, int delayTime) {
        this.delayTime = delayTime;
        this.cancelled = ifCancelled;
    }

    public int getDelayTime() {
        return this.delayTime;
    }

    public boolean isCancelled() {
        return this.cancelled;
    }
}
