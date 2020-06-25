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
    public ArrayList<Tuple2<String, Integer>> rankedList;
    public ArrayList<Tuple2<String, Integer>> rankedListAM;
    public ArrayList<Tuple2<String, Integer>> rankedListPM;
    public int interval;

    public DelayReason() {
    }

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

        String resultAM = "";
        String resultPM = "";

        if (rankedListAM != null) {

            for (Tuple2<String, Integer> item : rankedListAM)
                resultAM = resultAM + item._1 + ":" + item._2 + ",";
        }

        if (rankedListPM != null) {

            for (Tuple2<String, Integer> item : rankedListPM)
                resultPM = resultPM + "," + item._1 + ":" + item._2;
        }

        String result = formattedDate + ",(AM)," + resultAM + "(PM)" + resultPM;

        try{

            FileWriter fstream = new FileWriter("/home/luigi/IdeaProjects/project2/results/query2_results.csv",true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(result + "\n");
            out.close();

        }catch (Exception e){

            System.err.println("Error while writing to file: " + e.getMessage());

        }

        return result;
    }
}
