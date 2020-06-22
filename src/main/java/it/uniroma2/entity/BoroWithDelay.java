package it.uniroma2.entity;

import java.io.Serializable;

public class BoroWithDelay implements Serializable {

    public String outputDate;
    public long eventTime;
    public String boro;
    public long count;
    public long delay;
    public double average;

    public BoroWithDelay() {
    }

    public BoroWithDelay(long eventTime, String boro, long delay, long count) {
        this.eventTime = eventTime;
        this.boro = boro;
        this.delay = delay;
        this.count = count;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getBoro() {
        return boro;
    }

    public void setBoro(String boro) {
        this.boro = boro;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public String getOutputDate() {
        return outputDate;
    }

    public void setOutputDate(String outputDate) {
        this.outputDate = outputDate;
    }

    @Override
    public String toString() {
        return outputDate + "," + boro + "," + average;
    }
}