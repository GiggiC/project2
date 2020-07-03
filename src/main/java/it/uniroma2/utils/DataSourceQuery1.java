package it.uniroma2.utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

public class DataSourceQuery1 {

    public static void main(String[] args) throws InterruptedException {

        String csvFile = new File("data/bus-breakdown-and-delays.csv").getAbsolutePath();
        csvFile = csvFile.replaceAll("scripts/", "");
        String line;
        String cvsSplitBy = ";";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        int i = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                /*if (i % 50 == 0)
                    Thread.sleep(1);

                i++;*/

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
                    delay = delay.replaceAll("lg", "-6");
                    delay = delay.replaceAll("tt", "10");
                    delay = delay.replaceAll("ott", "10");
                    delay = delay.replaceAll("nov", "11");
                    delay = delay.replaceAll("dic", "12");
                    delay = delay.replaceAll("-m", "");
                    delay = delay.replaceAll("--m", "");
                    delay = delay.replace("+", "");
                    delay = delay.replace(".", "");
                    delay = delay.replaceAll("e", "");
                    delay = delay.replaceAll("i", "");
                    delay = delay.replaceAll("n", "");
                    delay = delay.replaceAll("o", "");
                    delay = delay.replaceAll("r", "");
                    delay = delay.replaceAll("s", "");
                    delay = delay.replaceAll("t", "");
                    delay = delay.replaceAll("u", "");

                    if (!delay.isEmpty() && !delay.replaceAll("[^0-9]", "").equals("0") && !delay.contains(":") && !delay.contains("/")) {

                        //field with only minutes
                        if (!delay.contains("h")) {

                            if (!delay.contains("-")) {

                                if (!delay.replaceAll("[^0-9]", "").isEmpty()) {

                                    result = formattedTime + "," + boro + "," + delay.replaceAll("[^0-9]", "");
                                    System.out.println(result);
                                }

                            } else {

                                if (!delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                    result = formattedTime + "," + boro + "," + delay.substring(delay.indexOf("-") + 1).replaceAll("[^0-9]", "");
                                    System.out.println(result);

                                } else {

                                    if (!delay.substring(0, delay.indexOf("-")).replaceAll("[^0-9]", "").isEmpty()) {

                                        result = formattedTime + "," + boro + "," + delay.substring(0, delay.indexOf("-")).replaceAll("[^0-9]", "");
                                        System.out.println(result);
                                    }
                                }
                            }

                        } else {

                            if (delay.contains("h") && !delay.contains("m") && !delay.replaceAll("[^0-9]", "").isEmpty()) {

                                result = formattedTime + "," + boro + "," + Integer.parseInt(delay.replaceAll("[^0-9]", "")) * 60;
                                System.out.println(result);

                            } else if (delay.contains("h") && delay.contains("m") && !delay.replaceAll("[^0-9]", "").isEmpty()) {

                                if (delay.indexOf("h") < delay.indexOf("m") && !delay.substring(0, delay.indexOf("h")).isEmpty()) {

                                    int min = Integer.parseInt(delay.substring(0, delay.indexOf("h"))) * 60 + Integer.parseInt(delay.substring(delay.indexOf("h") + 1).replaceAll("[^0-9]", ""));
                                    result = formattedTime + "," + boro + "," + min;
                                    System.out.println(result);

                                } else if (!delay.substring(delay.indexOf("m") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                    delay = delay.replaceAll("-", "");
                                    int min = Integer.parseInt(delay.substring(delay.indexOf("m") + 1).replaceAll("[^0-9]", "")) * 60;
                                    result = formattedTime + "," + boro + "," + min;
                                    System.out.println(result);
                                }

                            } else if (delay.contains("1/2")) {

                                result = formattedTime + "," + boro + "," + 30;
                                System.out.println(result);
                            }
                        }

                    } else if (delay.contains("/")) {

                        if (delay.substring(delay.indexOf("/")).contains("h")) {

                            delay = delay.substring(delay.indexOf("/") + 1);
                            int min = 0;

                            if (!delay.substring(delay.indexOf("h") + 1).replaceAll("[^0-9]", "").isEmpty()) {

                                min = Integer.parseInt(delay.substring(0, delay.indexOf("h"))) * 60 + Integer.parseInt(delay.substring(delay.indexOf("h") + 1));

                            } else {

                                min = Integer.parseInt(delay.substring(0, delay.indexOf("h"))) * 60;
                            }

                            result = formattedTime + "," + boro + "," + min;
                            System.out.println(result);
                        }
                    }
                } else {
                    //no boro
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
