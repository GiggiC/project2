package it.uniroma2.entity;

import scala.Tuple2;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DelayReason {

    public String outputDate;
    public long eventTime;
    public int interval;
    public ArrayList<Tuple2<String, Integer>> rankedListAM;
    public ArrayList<Tuple2<String, Integer>> rankedListPM;

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

    public ArrayList<Tuple2<String, Integer>> getRankedListAM() {
        return rankedListAM;
    }

    public void setRankedListAM(ArrayList<Tuple2<String, Integer>> rankedListAM) {
        this.rankedListAM = rankedListAM;
    }

    public ArrayList<Tuple2<String, Integer>> getRankedListPM() {
        return rankedListPM;
    }

    public void setRankedListPM(ArrayList<Tuple2<String, Integer>> rankedListPM) {
        this.rankedListPM = rankedListPM;
    }
}
