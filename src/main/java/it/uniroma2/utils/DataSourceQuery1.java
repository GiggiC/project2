package it.uniroma2.utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataSourceQuery1 {

    public static void main(String[] args) {

        String csvFile = new File("data/dataset.csv").getAbsolutePath();
        csvFile = csvFile.replaceAll("scripts/", "");
        String line = "";
        String cvsSplitBy = ";";

        ArrayList<String> arrayList = new ArrayList<>();
        ArrayList<String> other = new ArrayList<>();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        int i = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                if (i % 50 == 0)
                    Thread.sleep(1);

                i++;

                String[] splittedLine = line.split(cvsSplitBy);
                String result = "";

                if (!splittedLine[9].isEmpty()) {

                    Date d = sdf.parse(splittedLine[7]);
                    long formattedTime = d.getTime();
                    String boro = splittedLine[9];

                    String delay = splittedLine[11].toLowerCase();

                    if (!delay.isEmpty() && !delay.replaceAll("[^0-9]", "").isEmpty() && !delay.replaceAll("[^0-9]", "").equals("0")) {

                        //field with only minutes
                        //if (!delay.replaceAll("[^0-9]", "").isEmpty() && !delay.contains("h") && !delay.contains("/")) {
                        if (!delay.contains("h")) {

                            if (!delay.contains("-")) {

                                result = formattedTime + "," + boro + "," + delay.replaceAll("[^0-9]", "");
                                System.out.println(result);

                            } else {

                                if (!delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                    result = formattedTime + "," + boro + "," + delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "");
                                    System.out.println(result);
                                }

                            }

                        } else {

                            /*result = formattedTime + "," + boro + "," + delay;
                            other.add(result);
                            System.out.println(result);*/
                        }
                    }
                }
            }

        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }

        //System.out.println(arrayList.size());
        //System.out.println(other.size());
    }
}
