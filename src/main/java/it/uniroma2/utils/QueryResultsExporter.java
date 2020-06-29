package it.uniroma2.utils;

import java.io.*;

public class QueryResultsExporter {

    public static void main(String[] args) throws IOException {

        String output = args[0];

        InputStreamReader isReader = new InputStreamReader(System.in);
        BufferedReader bufReader = new BufferedReader(isReader);
        String inputStr;

        String csvFile = new File("results/" + output).getAbsolutePath();
        csvFile = csvFile.replaceAll("scripts/", "");

        FileWriter fstream = new FileWriter(csvFile, true);
        BufferedWriter out = new BufferedWriter(fstream);

        while ((inputStr = bufReader.readLine()) != null)
            out.write(inputStr + "\n");

        out.close();
    }
}
