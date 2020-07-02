package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Utils {

    public static BoroWithDelay csvParsingQuery1(String line) {

        String[] splittedLine = line.split(",");

        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        list.add(new Tuple2<>(splittedLine[1], Double.parseDouble(splittedLine[2])));

        BoroWithDelay result = new BoroWithDelay();
        result.setEventTime(Long.parseLong(splittedLine[0]));
        result.setCount(1);
        result.setDelay(Long.parseLong(splittedLine[2]));
        result.setBoroDelayAverageList(list);

        return result;
    }

    public static BoroWithDelay computeAverage(BoroWithDelay a, BoroWithDelay b) {

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setCount(a.getCount() + b.getCount());
        result.setDelay(a.getDelay() + b.getDelay());
        //result.setBoro(a.getBoro());
        //result.setAverage((double) result.getDelay() / result.getCount());

        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        list.add(new Tuple2<>(a.getBoroDelayAverageList().get(0)._1, (double) result.getDelay() / result.getCount()));
        result.setBoroDelayAverageList(list);

        return result;
    }

    public static BoroWithDelay inlineDate(BoroWithDelay a, BoroWithDelay b) {

        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        list.addAll(a.getBoroDelayAverageList());
        list.addAll(b.getBoroDelayAverageList());

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setBoroDelayAverageList(list);

        return result;
    }

    public static String boroResultMapper(BoroWithDelay boroWithDelay) {

        String result = formatDate(boroWithDelay.getOutputDate());

        for (Tuple2<String, Double> item: boroWithDelay.getBoroDelayAverageList())
            result = result + "," + item._1 + ":" + item._2;

        return result + "\n";
    }

    public static DelayReason csvParsingQuery2(String line) {

        String[] splittedLine = line.split(",");

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(splittedLine[0], 1));

        DelayReason result = new DelayReason();
        result.setEventTime(Long.parseLong(splittedLine[1]));
        result.setInterval(Integer.parseInt(splittedLine[2]));
        result.setRankedList(list);

        if (result.getInterval() == 1) {

            result.setRankedListAM(result.getRankedList());
            return result;
        }

        result.setRankedListPM(result.getRankedList());

        return result;

    }

    public static DelayReason delayReasonCount(DelayReason a, DelayReason b) {

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(a.getRankedList().get(0)._1, a.getRankedList().get(0)._2 + b.getRankedList().get(0)._2));
        a.setRankedList(list);

        return a;
    }

    public static DelayReason multipleIntervalReducer(DelayReason a, DelayReason b, int interval) {

        a.getRankedList().addAll(b.getRankedList());

        for (int i = 0; i < a.getRankedList().size(); i++) {

            for (int j = 0; j < a.getRankedList().size(); j++) {

                if (a.getRankedList().get(i)._1.equals(a.getRankedList().get(j)._1) && i != j) {

                    a.getRankedList().set(i, new Tuple2<>(a.getRankedList().get(i)._1, a.getRankedList().get(i)._2 + a.getRankedList().get(j)._2));
                    a.getRankedList().remove(j);
                }
            }
        }

        TupleComparator comparator = new TupleComparator();
        a.getRankedList().sort(comparator);

        if (a.getRankedList().size() > 2)
            a.getRankedList().subList(3, a.getRankedList().size()).clear();

        if (interval == 1) {

            a.setRankedListAM(a.getRankedList());
            return a;
        }

        a.setRankedListPM(a.getRankedList());

        return a;
    }

    public static DelayReason streamsUnion(DelayReason a, DelayReason b) {

        DelayReason delayReason = new DelayReason();
        delayReason.setOutputDate(a.getOutputDate());

        if (a.getInterval() == 1) {

            delayReason.setRankedListAM(a.getRankedList());
            delayReason.setRankedListPM(b.getRankedList());
            return delayReason;
        }

        delayReason.setRankedListAM(b.getRankedList());
        delayReason.setRankedListPM(a.getRankedList());

        return delayReason;
    }

    public static String delaReasonResultMapper(DelayReason delayReason) {

        String formattedDate = formatDate(delayReason.getOutputDate());

        String resultAM = "";
        String resultPM = "";

        if (delayReason.getRankedListAM() != null) {

            for (Tuple2<String, Integer> item : delayReason.getRankedListAM())
                resultAM = resultAM + item._1 + ":" + item._2 + ",";
        }

        if (delayReason.getRankedListPM() != null) {

            for (Tuple2<String, Integer> item : delayReason.getRankedListPM())
                resultPM = resultPM + "," + item._1 + ":" + item._2;
        }

        return formattedDate + ",(AM)," + resultAM + "(PM)" + resultPM + "\n";
    }

    public static String formatDate(String date) {

        Date outDate = new Date(Long.parseLong(date));

        SimpleDateFormat formatnow = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date outDateString = null;

        try {

            outDateString = formatnow.parse(String.valueOf(outDate));

        } catch (ParseException e) {

            e.printStackTrace();
        }

        return outFormat.format(outDateString);
    }
}