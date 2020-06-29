package it.uniroma2.utils;

import java.io.*;

public class QueryResultsExporter {

    public static void main(String[] args) throws IOException {

        String output = args[0];

        InputStreamReader isReader = new InputStreamReader(System.in);
        BufferedReader bufReader = new BufferedReader(isReader);
        String inputStr;

        while ((inputStr = bufReader.readLine()) != null) {

            FileWriter fstream = new FileWriter("/home/luigi/IdeaProjects/project2/results/" + output, true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(inputStr + "\n");
            out.close();
        }
    }
}
