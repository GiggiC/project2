package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
    static SimpleDateFormat output = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static BoroWithDelay csvParsingQuery1(String line) throws ParseException {

        String[] splittedLine = line.split(",");
        Date d = sdf.parse(String.valueOf(new Date(Long.parseLong(splittedLine[0]))));
        String formattedTime = output.format(d);

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
        String formattedTime = output.format(d);

        BoroWithDelay result = new BoroWithDelay();
        result.setOutputDate(formattedTime);
        result.setEventTime(a.getEventTime());
        result.setBoro(a.getBoro());
        result.setCount(a.getCount() + b.getCount());
        result.setDelay(a.getDelay() + b.getDelay());
        result.setAverage((double) result.getDelay()/result.getCount());

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
        String formattedTime = output.format(d);

        DelayReason result = new DelayReason();
        result.setOutputDate(formattedTime);
        result.setDelayReason(splittedLine[0]);
        result.setEventTime(Long.parseLong(splittedLine[1]));
        result.setDelayReasonCount(1);
        result.setInterval(Integer.parseInt(splittedLine[2]));

        return result;
    }
}
