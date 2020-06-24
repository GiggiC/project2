package it.uniroma2.entity;

import scala.Tuple2;
import java.util.ArrayList;

public class DelayReason {

    public String outputDate;
    public long eventTime;
    public ArrayList<Tuple2<String, Integer>> rankedList;
    public int interval;
    public String outputString;

    public DelayReason() {}

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
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

    public ArrayList<Tuple2<String, Integer>> getRankedList() {
        return rankedList;
    }

    public void setRankedList(ArrayList<Tuple2<String, Integer>> rankedList) {
        this.rankedList = rankedList;
    }

    public String getOutputString() {
        return outputString;
    }

    public void setOutputString(String outputString) {
        this.outputString = outputString;
    }

    @Override
    public String toString() {

        return outputString;
    }

    /*@Override
    public String toString() {

        return outputDate + "," + rankedList + "," + interval;
    }*/
}
