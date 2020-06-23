package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import scala.Tuple2;
import scala.collection.immutable.Stream;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

public class Utils {

    static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
    static SimpleDateFormat outputQuery1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat outputQuery2 = new SimpleDateFormat("yyyy-MM-dd");

    public static BoroWithDelay csvParsingQuery1(String line) throws ParseException {

        String[] splittedLine = line.split(",");
        Date d = sdf.parse(String.valueOf(new Date(Long.parseLong(splittedLine[0]))));
        String formattedTime = outputQuery1.format(d);

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(formattedTime);
        result.setEventTime(Long.parseLong(splittedLine[0]));
        result.setBoro(splittedLine[1]);
        result.setCount(1);
        result.setDelay(Long.parseLong(splittedLine[2]));
        result.setAverage(Double.parseDouble(splittedLine[2]));

        return result;
    }

    public static BoroWithDelay computeAverage(BoroWithDelay a, BoroWithDelay b) throws ParseException {

        Date d = sdf.parse(String.valueOf(new Date(b.getEventTime())));
        String formattedTime = outputQuery1.format(d);

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(formattedTime);
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
        result.setBoro(a.getBoro() + ":" + a.getDelay() + "," + b.getBoro() + ":" + b.getDelay());

        return result;
    }

    public static DelayReason csvParsingQuery2(String line) throws ParseException {

        String[] splittedLine = line.split(",");
        Date d = sdf.parse(String.valueOf(new Date(Long.parseLong(splittedLine[1]))));
        String formattedTime = outputQuery2.format(d);

        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(splittedLine[0], 1));

        DelayReason result = new DelayReason();
        result.setOutputDate(formattedTime);
        result.setEventTime(Long.parseLong(splittedLine[1]));
        result.setRankedList(list);
        result.setInterval(Integer.parseInt(splittedLine[2]));

        return result;
    }

    public static DelayReason delayReasonCount(DelayReason a, DelayReason b) {

        ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
        result.add(new Tuple2<>(a.getRankedList().get(0)._1, a.getRankedList().get(0)._2 + b.getRankedList().get(0)._2));

        a.setRankedList(result);

        return a;
    }

    public static DelayReason intervalReducer(DelayReason a, DelayReason b) {

        a.getRankedList().addAll(b.getRankedList());
        TupleComparator comparator = new TupleComparator();

        a.getRankedList().sort(comparator);

        if (a.getRankedList().size() > 2)
            a.getRankedList().subList(3, a.getRankedList().size()).clear();

        return a;
    }
}
