package it.uniroma2.utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataSourceQuery2 {

    public static void main(String[] args) throws ParseException {

        String csvFile = new File("data/dataset.csv").getAbsolutePath();
        csvFile = csvFile.replaceAll("scripts/", "");
        String line;
        String cvsSplitBy = ";";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        SimpleDateFormat output1 = new SimpleDateFormat("HH:mm:ss");

        Date firstIntervalStart = output1.parse("5:00:00");
        Date firstIntervalEnd = output1.parse("12:00:00");//TODO

        Date secondIntervalStart = output1.parse("12:00:00");
        Date secondIntervalEnd = output1.parse("19:00:00");

        int i = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                if (i % 50 == 0)
                    Thread.sleep(1);

                i++;

                String[] splittedLine = line.split(cvsSplitBy);
                Date d = sdf.parse(splittedLine[7]);
                long formattedTime = d.getTime();

                String str = output1.format(d);
                Date d2 = output1.parse(str);

                String result;

                if (d2.compareTo(firstIntervalStart) > 0 && d2.compareTo(firstIntervalEnd) < 0) {

                    result = splittedLine[5] + "," + formattedTime + "," + 1;
                    System.out.println(result);
                }

                if (d2.compareTo(secondIntervalStart) > 0 && d2.compareTo(secondIntervalEnd) < 0) {

                    result = splittedLine[5] + "," + formattedTime + "," + 2;
                    System.out.println(result);
                }
            }

        } catch (IOException | ParseException | InterruptedException e) {

            e.printStackTrace();
        }
    }
}
