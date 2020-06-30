package it.uniroma2;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.utils.ProcessingWindowQuery1;
import it.uniroma2.utils.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

@SuppressWarnings("serial")
public class Query1 {

    public static void main(String[] args) throws Exception {

        // the host and the inputPort to connect to
        final String hostname;
        final int inputPort, exportPort, numDays;

        try {

            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            inputPort = params.has("inputPort") ? params.getInt("inputPort") : 9091;
            exportPort = params.has("exportPort") ? params.getInt("exportPort") : 9001;
            numDays = params.getInt("numDays");

        } catch (Exception e) {

            System.err.println("Error passing arguments. Please run 'Query1 " +
                    "--hostname <hostname> --inputPort <inputPort> --exportPort <exportPort> --numDays <numDays>', " +
                    "where hostname (localhost by default), inputPort (9091 by default), exportPort (9001 by default)");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, inputPort, "\n");

        DataStream<String> result = text

                .map(Utils::csvParsingQuery1)

                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BoroWithDelay>() {
                    @Override
                    public long extractAscendingTimestamp(BoroWithDelay boroWithDelay) {
                        return boroWithDelay.getEventTime();
                    }
                })

                .keyBy((KeySelector<BoroWithDelay, String>) boroWithDelay -> boroWithDelay.boro)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::computeAverage, new ProcessingWindowQuery1())
                .keyBy((KeySelector<BoroWithDelay, String>) boroWithDelay -> boroWithDelay.outputDate)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::inlineDate)
                .map(Utils::boroResultMapper);

        result.writeToSocket(hostname, exportPort, String::getBytes);

        env.execute("Query1");
    }
}

