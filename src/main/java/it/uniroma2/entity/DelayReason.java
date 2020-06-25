package it.uniroma2.entity;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

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

        Date expiry = new Date(Long.parseLong(outputDate));

        SimpleDateFormat formatnow = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
        SimpleDateFormat formatneeded=new SimpleDateFormat("yyyy-MM-dd");

        Date date1 = null;

        try {
            date1 = formatnow.parse(String.valueOf(expiry));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String date2 = formatneeded.format(date1);

        return date2 + "," + rankedList + "," + interval;
    }*/
}
