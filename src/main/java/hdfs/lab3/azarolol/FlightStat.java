package hdfs.lab3.azarolol;

import scala.Serializable;

public class FlightStat implements Serializable {
    private final boolean cancelled;
    private final float delayTime;

    public FlightStat(boolean ifCancelled, float delayTime) {
        this.delayTime = delayTime;
        this.cancelled = ifCancelled;
    }

    public float getDelayTime() {
        return this.delayTime;
    }

    public boolean isCancelled() {
        return this.cancelled;
    }
}
