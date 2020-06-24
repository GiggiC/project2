package it.uniroma2.utils;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataSourceQuery1 {

    public static void main(String[] args) {

        String csvFile = "/home/luigi/IdeaProjects/project2/data/dataset.csv";
        String outputCsv = "/home/luigi/IdeaProjects/project2/data/outputCSVQuery1.csv";
        String line = "";
        String cvsSplitBy = ";";

        ArrayList<String> arrayList = new ArrayList<>();
        ArrayList<String> other = new ArrayList<>();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                String[] splittedLine = line.split(cvsSplitBy);

                if (!splittedLine[9].isEmpty()) {

                    Date d = sdf.parse(splittedLine[7]);
                    long formattedTime = d.getTime();
                    String boro = splittedLine[9].replaceAll(" ", "-");

                    String delay = splittedLine[11].toLowerCase();

                    //field with only minutes
                    if (!delay.replaceAll("[^0-9]", "").isEmpty() && !delay.contains("h") && !delay.contains("/")) {

                        if (delay.contains("-")) {

                            String[] splitted = delay.split("-");
                            int max = 0;

                            for (String item : splitted) {

                                if (!item.replaceAll("[^0-9]", "").isEmpty()) {

                                    int value = Integer.parseInt(item.replaceAll("[^0-9]", ""));

                                    if (value > max)
                                        max = value;

                                    line = formattedTime + "," + splittedLine[7].substring(0, 10)+ "," + boro + "," + max;

                                }

                            }

                        } else if (delay.contains("/")) {

                            String[] splitted = delay.split("/");
                            int max = 0;

                            for (String item : splitted) {

                                if (!item.replaceAll("[^0-9]", "").isEmpty()) {

                                    int value = Integer.parseInt(item.replaceAll("[^0-9]", ""));

                                    if (value > max)
                                        max = value;

                                    line = formattedTime + "," + splittedLine[7].substring(0, 10) + "," + boro + "," + max;

                                }

                            }

                        } else {

                            line = formattedTime+ "," + splittedLine[7].substring(0, 10) + "," + boro + "," + delay.replaceAll("[^0-9]", "");
                        }

                        arrayList.add(line);

                    } else if (!delay.replaceAll("[^0-9]", "").isEmpty() && delay.contains("h") && !splittedLine[11].contains("?")) {

                        if (splittedLine[11].equals("1 hour") || splittedLine[11].equals("1hour") || splittedLine[11].equals("1 hourr") || splittedLine[11].equals("1hr") || splittedLine[11].equals("1 hr")
                                || splittedLine[11].equals("45min/1hr") || splittedLine[11].equals("45min-1hr") || splittedLine[11].equals("45min-1 hr")
                                || splittedLine[11].equals("45min 1hr") || splittedLine[11].equals("45-1hr") || splittedLine[11].equals("45mins 1hr")
                                || splittedLine[11].equals("45 min-1hr") || splittedLine[11].equals("45mini/1hr") || splittedLine[11].equals("45min -1hr") || splittedLine[11].equals("45min - 1h")
                                || splittedLine[11].equals("45/1hour") || splittedLine[11].equals("45 -1 hour") || splittedLine[11].equals("45 min-1 h") || splittedLine[11].equals("45-1hour")) {

                            line = formattedTime + "," + splittedLine[7].substring(0, 10) + "," + boro + "," + "60";
                            arrayList.add(line);

                        } else {

                            line = formattedTime + "," + splittedLine[7].substring(0, 10)+ "," + splittedLine[9] + "," + splittedLine[11];
                            other.add(line);
                        }

                    } else {

                        line = formattedTime + "," + splittedLine[7].substring(0, 10) + "," + splittedLine[9] + "," + splittedLine[11];
                        other.add(line);
                    }

                /*else if (splittedLine[11].isEmpty()) {

                    arrayList.add(line);
                    empty++;

                } else {

                    line = splittedLine[7] + "," + splittedLine[9] + "," + splittedLine[11];
                    System.out.println(line);
                    rest++;
                }*/
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        System.out.println(arrayList.size());
        System.out.println(other.size());

        for (String item : other) {

            //System.out.println(item);
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
