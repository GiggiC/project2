package it.uniroma2.utils;

import java.io.*;

public class QueryResultsExporter {

    public static void main(String[] args) {

        String output = args[0];

        InputStreamReader isReader = new InputStreamReader(System.in);
        BufferedReader bufReader = new BufferedReader(isReader);

        while (true) {

            try {
                String inputStr = null;

                if ((inputStr = bufReader.readLine()) != null) {

                    FileWriter fstream = new FileWriter("/home/luigi/IdeaProjects/project2/results/" + output, true);
                    BufferedWriter out = new BufferedWriter(fstream);
                    out.write(inputStr + "\n");
                    out.close();
                }

            } catch (IOException e) {

                e.printStackTrace();
            }
        }
    }
}