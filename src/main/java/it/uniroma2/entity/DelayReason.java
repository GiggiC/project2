package it.uniroma2.entity;

public class DelayReason {

    public String outputDate;
    public long eventTime;
    public String delayReason;
    public int delayReasonCount;
    public int interval;

    public DelayReason() {
    }

    public DelayReason(String outputDate, long eventTime, String delayReason, int delayReasonCount, int interval) {
        this.outputDate = outputDate;
        this.eventTime = eventTime;
        this.delayReason = delayReason;
        this.delayReasonCount = delayReasonCount;
        this.interval = interval;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getDelayReason() {
        return delayReason;
    }

    public void setDelayReason(String delayReason) {
        this.delayReason = delayReason;
    }

    public int getDelayReasonCount() {
        return delayReasonCount;
    }

    public void setDelayReasonCount(int delayReasonCount) {
        this.delayReasonCount = delayReasonCount;
    }

    public String getOutputDate() {
        return outputDate;
    }

    public void setOutputDate(String outputDate) {
        this.outputDate = outputDate;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return outputDate + "," + delayReason + "," + delayReasonCount + "," + interval;
    }
}
