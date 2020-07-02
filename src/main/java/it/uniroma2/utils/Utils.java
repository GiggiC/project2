package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Utils {

    /**
     * @param line string to parse
     * @return parsed entity
     */
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

    /**
     * @param a boro a keyed by boro
     * @param b boro b keyed by boro
     * @return reduced entity with total count, delay and average
     */
    public static BoroWithDelay computeAverage(BoroWithDelay a, BoroWithDelay b) {

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setCount(a.getCount() + b.getCount());
        result.setDelay(a.getDelay() + b.getDelay());

        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        list.add(new Tuple2<>(a.getBoroDelayAverageList().get(0)._1, (double) result.getDelay() / result.getCount()));
        result.setBoroDelayAverageList(list);

        return result;
    }

    /**
     * @param a boro a keyed by date
     * @param b boro b keyed by date
     * @return reduced entity with query result
     */
    public static BoroWithDelay inlineDate(BoroWithDelay a, BoroWithDelay b) {

        ArrayList<Tuple2<String, Double>> list = new ArrayList<>();
        list.addAll(a.getBoroDelayAverageList());
        list.addAll(b.getBoroDelayAverageList());

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setBoroDelayAverageList(list);

        return result;
    }

    /**
     * @param boroWithDelay query result line
     * @return formatted csv query result
     */
    public static String boroResultMapper(BoroWithDelay boroWithDelay) {

        String result = formatDate(boroWithDelay.getOutputDate());

        for (Tuple2<String, Double> item : boroWithDelay.getBoroDelayAverageList())
            result = result + "," + item._1 + ":" + item._2;

        return result + "\n";
    }

    /**
     * @param line string to parse
     * @return parsed entity
     */
    public static DelayReason csvParsingQuery2(String line) {

        String[] splittedLine = line.split(",");

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(splittedLine[0], 1));

        DelayReason result = new DelayReason();
        result.setEventTime(Long.parseLong(splittedLine[1]));
        result.setInterval(Integer.parseInt(splittedLine[2]));

        if (result.getInterval() == 1) {

            result.setRankedListAM(list);

        } else {

            result.setRankedListPM(list);
        }

        return result;

    }

    /**
     * @param a delayReason a keyed by reason
     * @param b delayReason b keyed by reason
     * @return reduced entity with total count
     */
    public static DelayReason delayReasonCount(DelayReason a, DelayReason b, int interval) {

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        DelayReason result = new DelayReason();

        if (interval == 1) {

            list.add(new Tuple2<>(a.getRankedListAM().get(0)._1, a.getRankedListAM().get(0)._2 + b.getRankedListAM().get(0)._2));
            result.setRankedListAM(list);
            result.setInterval(1);

        } else {

            list.add(new Tuple2<>(a.getRankedListPM().get(0)._1, a.getRankedListPM().get(0)._2 + b.getRankedListPM().get(0)._2));
            result.setRankedListPM(list);
            result.setInterval(2);
        }

        return result;
    }

    /**
     * @param a delayReason a keyed by date
     * @param b delayReason b keyed by date
     * @param interval AM or PM
     * @return reduced entity with sorted ranked list per date
     */
    public static DelayReason multipleIntervalReducer(DelayReason a, DelayReason b, int interval) {

        DelayReason result = new DelayReason();
        result.setOutputDate(a.getOutputDate());

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();

        if (interval == 1) {

            list.addAll(a.getRankedListAM());
            list.addAll(b.getRankedListAM());

        } else {

            list.addAll(a.getRankedListPM());
            list.addAll(b.getRankedListPM());
        }

        //reduce occurrences of same delay reasons
        for (int i = 0; i < list.size(); i++) {

            for (int j = 0; j < list.size(); j++) {

                if (list.get(i)._1.equals(list.get(j)._1) && i != j) {

                    list.set(i, new Tuple2<>(list.get(i)._1, list.get(i)._2 + list.get(j)._2));
                    list.remove(j);
                }
            }
        }

        TupleComparator comparator = new TupleComparator();
        list.sort(comparator);

        //reduce delay reasons with equal number of occurrences
        for (int i = 0; i < list.size() - 1; i++) {

            if (list.get(i)._2.equals(list.get(i + 1)._2)) {

                list.set(i, new Tuple2<>(list.get(i)._1 + "-" + list.get(i + 1)._1, list.get(i)._2));
                list.remove(i + 1);
            }
        }

        if (list.size() > 2)
            list.subList(3, list.size()).clear();


        if (interval == 1) {

            result.setRankedListAM(list);
            result.setInterval(1);

        } else {

            result.setRankedListPM(list);
            result.setInterval(2);
        }

        return result;
    }

    /**
     * @param a delayReason a
     * @param b delayReason b
     * @return entity with AM and PM info
     */
    public static DelayReason streamsUnion(DelayReason a, DelayReason b) {

        DelayReason result = new DelayReason();
        result.setOutputDate(a.getOutputDate());

        if (a.getInterval() == 1) {

            result.setRankedListAM(a.getRankedListAM());
            result.setRankedListPM(b.getRankedListPM());

        } else {

            result.setRankedListAM(b.getRankedListAM());
            result.setRankedListPM(a.getRankedListPM());
        }

        return result;
    }

    /**
     * @param delayReason query result line
     * @return formatted csv query result
     */
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

    /**
     * @param date date to format
     * @return formatted output csv date
     */
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