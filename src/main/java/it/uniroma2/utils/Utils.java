package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

import java.text.ParseException;
import java.util.ArrayList;

public class Utils {

    public static BoroWithDelay csvParsingQuery1(String line) {

        String[] splittedLine = line.split(",");

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(splittedLine[1]);
        result.setEventTime(Long.parseLong(splittedLine[0]));
        result.setBoro(splittedLine[2]);
        result.setCount(1);
        result.setDelay(Long.parseLong(splittedLine[3]));
        result.setAverage(Double.parseDouble(splittedLine[3]));

        return result;
    }

    public static BoroWithDelay computeAverage(BoroWithDelay a, BoroWithDelay b) {

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setEventTime(a.getEventTime());
        result.setBoro(a.getBoro());
        result.setCount(a.getCount() + b.getCount());
        result.setDelay(a.getDelay() + b.getDelay());
        result.setAverage((double) result.getDelay() / result.getCount());

        return result;
    }

    public static BoroWithDelay inlineDate(BoroWithDelay a, BoroWithDelay b) {

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(a.getOutputDate());
        result.setBoro(a.getBoro() + ":" + a.getAverage() + "," + b.getBoro() + ":" + b.getAverage());

        return result;
    }

    public static DelayReason csvParsingQuery2(String line) {

        String[] splittedLine = line.split(",");

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(splittedLine[0], 1));

        DelayReason result = new DelayReason();
        result.setEventTime(Long.parseLong(splittedLine[1]));
        result.setOutputDate(splittedLine[2]);
        result.setInterval(Integer.parseInt(splittedLine[3]));
        result.setRankedList(list);

        return result;
    }

    public static DelayReason delayReasonCount(DelayReason a, DelayReason b) {

        ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
        result.add(new Tuple2<>(a.getRankedList().get(0)._1, a.getRankedList().get(0)._2 + b.getRankedList().get(0)._2));

        a.setRankedList(result);

        return a;
    }

    public static DelayReason multipleIntervalReducer(DelayReason a, DelayReason b) {

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

        return a;
    }

    public static DelayReason outputIntervalReducer(DelayReason a, DelayReason b) {

        a.getRankedList().addAll(b.getRankedList());

        return a;
    }

    public static DelayReason stremsUnion(DelayReason a, DelayReason b) {

        String resultA = "";
        String resultB = "";
        String result1 = "";

        if (a.getRankedList() != null) {

            for (int i = 0; i < a.getRankedList().size(); i++)
                resultA = resultA + a.getRankedList().get(i)._1 + ":" + a.getRankedList().get(i)._2 + ",";
        }

        if (b.getRankedList() != null) {

            for (int i = 0; i < b.getRankedList().size(); i++)
                resultB = resultB + "," + b.getRankedList().get(i)._1 + ":" + b.getRankedList().get(i)._2;
        }

        result1 = a.outputDate + ",5:00-11:59," + resultA + "12:00-19:00" + resultB;

        DelayReason delayReason = new DelayReason();
        delayReason.setOutputString(result1);

        return delayReason;
    }
}