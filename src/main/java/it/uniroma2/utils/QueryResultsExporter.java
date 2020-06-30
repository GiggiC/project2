package it.uniroma2.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;

public class QueryResultsExporter {

    public static void main(String[] args) throws IOException {

        int query = 0;
        int numDays = 0;
        String output;

        try {

            query = Integer.parseInt(args[0]);
            numDays = Integer.parseInt(args[1]);

        } catch (Exception e) {

            System.out.println(query + " " + numDays);
            System.err.println("Error passing arguments. Please run script with args: " +
                    "<queryNum (1 or 2)> <numDays (query1: 1,7,30 - query2: 1,7)>");
            return;
        }

        if (query == 1 && numDays == 1) {

            output = "query1_1day_results.csv";

        } else if (query == 1 && numDays == 7) {

            output = "query1_7days_results.csv";

        } else if (query == 1 && numDays == 30) {

            output = "query1_30days_results.csv";

        } else if (query == 2 && numDays == 1) {

            output = "query2_1days_results.csv";

        } else if (query == 2 && numDays == 7) {

            output = "query2_7days_results.csv";

        } else {

            System.err.println("Error passing arguments. Please run 'QueryResultsExportes " +
                    "--queryNum <queryNum (1 or 2)> --numDays <numDays (query1: 1,7,30 - query2: 1,7)>");
            return;
        }

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
