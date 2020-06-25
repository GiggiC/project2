package it.uniroma2.entity;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BoroWithDelay implements Serializable {

    public String outputDate;
    public long eventTime;
    public String boro;
    public long count;
    public long delay;
    public String average;

    public BoroWithDelay() {}

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

    public String getAverage() {
        return average;
    }

    public void setAverage(String average) {
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

        Date outDate = new Date(Long.parseLong(outputDate));

        SimpleDateFormat formatnow = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date outDateString = null;

        try {

            outDateString = formatnow.parse(String.valueOf(outDate));

        } catch (ParseException e) {

            e.printStackTrace();
        }

        String formattedDate = outFormat.format(outDateString);

        String[] splittedBoro = boro.split(",");
        String[] splittedAverage = average.split(",");

        String result = formattedDate;

        for (int i = 0; i < splittedBoro.length; i++)
            result = result + "," + splittedBoro[i] + ":" + splittedAverage[i];

        try {

            FileWriter fstream = new FileWriter("/home/luigi/IdeaProjects/project2/results/query1_results.csv", true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(result + "\n");
            out.close();

        } catch (Exception e) {

            System.err.println("Error while writing to file: " + e.getMessage());
        }

        return formattedDate + result;
    }
}