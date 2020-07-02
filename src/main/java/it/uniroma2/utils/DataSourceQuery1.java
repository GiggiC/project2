package it.uniroma2.utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataSourceQuery1 {

    public static void main(String[] args) throws InterruptedException {

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

                String[] splittedLine = line.split(cvsSplitBy);
                String result = "";

                if (!splittedLine[9].isEmpty()) {

                    Date d = sdf.parse(splittedLine[7]);
                    long formattedTime = d.getTime();
                    String boro = splittedLine[9];

                    String delay = splittedLine[11].toLowerCase().replaceAll(" ", "");
                    delay = delay.replaceAll("late", "");
                    delay = delay.replaceAll("lat", "");
                    delay = delay.replaceAll("half", "30");
                    delay = delay.replaceAll("one", "1");
                    delay = delay.replaceAll("feb", "2");
                    delay = delay.replaceAll("lg", "6");
                    delay = delay.replaceAll("tt", "10");
                    delay = delay.replaceAll("ott", "10");
                    delay = delay.replaceAll("nv", "11");
                    delay = delay.replaceAll("dic", "12");
                    delay = delay.replace("?", "");
                    delay = delay.replace("+", "");
                    delay = delay.replace(".", "");
                    delay = delay.replaceAll("s", "");
                    delay = delay.replaceAll("o", "");
                    delay = delay.replaceAll("u", "");
                    delay = delay.replaceAll("r", "");
                    delay = delay.replaceAll("i", "");
                    delay = delay.replaceAll("n", "");
                    delay = delay.replaceAll("t", "");

                    if (!delay.isEmpty() && !delay.replaceAll("[^0-9]", "").equals("0") && !delay.contains(":")) {

                        //field with only minutes
                        //if (!delay.replaceAll("[^0-9]", "").isEmpty() && !delay.contains("h") && !delay.contains("/")) {
                        if (!delay.contains("h")) {

                            if (!delay.contains("-")) {

                                delay = delay.replaceAll("[^0-9]", "");

                                if (!delay.isEmpty()) {

                                    result = formattedTime + "," + boro + "," + delay;
                                    //arrayList.add(result);
                                    System.out.println(result);

                                }

                            } else {

                                if (!delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                    result = formattedTime + "," + boro + "," + delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "");
                                    //arrayList.add(result);
                                    System.out.println(result);

                                } else {
                                    //TODO
                                    //other.add(delay);

                                }

                            }

                        } else {

                            String removedNumbers = delay.replaceAll("[0-9]", "");

                            if (removedNumbers.equals("h") && !delay.replaceAll("[^0-9]", "").isEmpty()) {

                                result = formattedTime + "," + boro + "," + Integer.parseInt(delay.replaceAll("[^0-9]", "")) * 60;
                                //arrayList.add(result);
                                System.out.println(result);

                            } else if (delay.contains("1/2")) {

                                result = formattedTime + "," + boro + "," + 30;
                                //arrayList.add(result);
                                System.out.println(result);

                            } else if (delay.contains("/")) {
                                //TODO
                                //other.add(delay);

                            } else if (delay.contains("-")) {

                                if (!delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                    result = formattedTime + "," + boro + "," + Integer.parseInt(delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "")) * 60;
                                    //arrayList.add(result);
                                    System.out.println(result);

                                } else {

                                    result = formattedTime + "," + boro + "," + 60;
                                    //arrayList.add(result);
                                    System.out.println(result);
                                }

                            } else {

                                other.add(delay);

                            }
                        }
                    } else {


                        //other.add(delay); NON MI IMPORTA SONO VUOTI O 0 42618 //TODO :

                    }
                } else {

                    //other.add(line); NON MI IMPORTA NO QUARTIERE 11095

                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

//        System.out.println(other);
//        System.out.println(arrayList.size());
//        System.out.println(other.size());
    }
}
