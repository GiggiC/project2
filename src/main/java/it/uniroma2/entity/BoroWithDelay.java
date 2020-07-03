package it.uniroma2.entity;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

public class BoroWithDelay implements Serializable {

    public String outputDate;
    public long eventTime;
    public long count;
    public long delay;

    //tuple<boro, average>
    public ArrayList<Tuple2<String, Double>> boroDelayAverageList;

    public BoroWithDelay() {}

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
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

    public String getOutputDate() {
        return outputDate;
    }

    public void setOutputDate(String outputDate) {
        this.outputDate = outputDate;
    }

    public ArrayList<Tuple2<String, Double>> getBoroDelayAverageList() {
        return boroDelayAverageList;
    }

    public void setBoroDelayAverageList(ArrayList<Tuple2<String, Double>> boroDelayAverageList) {
        this.boroDelayAverageList = boroDelayAverageList;
    }
}