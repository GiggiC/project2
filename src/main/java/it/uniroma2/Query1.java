package it.uniroma2;

import it.uniroma2.entity.BoroWithDelay;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import static it.uniroma2.utils.Utils.*;

@SuppressWarnings("serial")
public class Query1 {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port, numDays;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
            numDays = params.getInt("numDays");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port> --numDays <numDays>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<BoroWithDelay> boroWithAverage = text

                .map(line -> csvParsingQuery1(line))

                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BoroWithDelay>() {
                    @Override
                    public long extractAscendingTimestamp(BoroWithDelay boroWithDelay) {
                        return boroWithDelay.getEventTime();
                    }
                })

                .keyBy((KeySelector<BoroWithDelay, String>) boroWithDelay -> boroWithDelay.boro)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> computeAverage(a, b))
                .keyBy((KeySelector<BoroWithDelay, String>) boroWithDelay -> boroWithDelay.outputDate)
                .reduce((a, b) -> inlineDate(a, b));

        boroWithAverage.print();

        env.execute("Query1");
    }
}
