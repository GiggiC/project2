package it.uniroma2.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataSourceQuery2 {

    public static void main(String[] args) throws ParseException {

        String csvFile = "/home/luigi/IdeaProjects/project2/data/dataset.csv";
        String outputCsv = "/home/luigi/IdeaProjects/project2/data/outputCSVQuery2.csv";
        String line = "";
        String cvsSplitBy = ";";

        ArrayList<String> arrayList = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        SimpleDateFormat output = new SimpleDateFormat("HH:mm:ss");

        Date firstIntervalStart = output.parse("5:00:00");
        Date firstIntervalEnd = output.parse("12:00:00");

        Date secondIntervalStart = output.parse("12:00:00");
        Date secondIntervalEnd = output.parse("19:00:00");

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                String[] splittedLine = line.split(cvsSplitBy);
                Date d = sdf.parse(splittedLine[7]);
                long formattedTime = d.getTime();

                String str = output.format(d);
                Date d2 = output.parse(str);

                String result;

                if (d2.compareTo(firstIntervalStart) > 0 && d2.compareTo(firstIntervalEnd) < 0) {

                    result = splittedLine[5] + "," + formattedTime + "," + 1;
                    arrayList.add(result);
                }

                if (d2.compareTo(secondIntervalStart) > 0 && d2.compareTo(secondIntervalEnd) < 0) {

                    result = splittedLine[5] + "," + formattedTime + "," + 2;
                    arrayList.add(result);
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        FileWriter writer = null;

        try {

            writer = new FileWriter(outputCsv);

            for (String item : arrayList) {

                writer.append(item);
                writer.append("\n");
            }

            writer.flush();
            writer.close();

        } catch (IOException e) {

            e.printStackTrace();
        }
    }
}
