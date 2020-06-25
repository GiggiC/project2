package it.uniroma2;

import it.uniroma2.entity.DelayReason;
import it.uniroma2.utils.ProcessingWindowQuery2;
import it.uniroma2.utils.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static it.uniroma2.utils.Utils.*;

@SuppressWarnings("serial")
public class Query2 {

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
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<DelayReason> outputStreamOperator = text

                .map(Utils::csvParsingQuery2)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DelayReason>() {
                    @Override
                    public long extractAscendingTimestamp(DelayReason delayReason) {
                        return delayReason.getEventTime();
                    }
                });

        DataStream<DelayReason> outputStreamOperatorInterval1 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 1)
                .keyBy((KeySelector<DelayReason, Object>) delayReason -> delayReason.rankedList.get(0)._1)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::delayReasonCount, new ProcessingWindowQuery2())
                .keyBy(delayReason -> delayReason.outputDate)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> multipleIntervalReducer(a, b, 1));

        DataStream<DelayReason> outputStreamOperatorInterval2 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 2)
                .keyBy((KeySelector<DelayReason, Object>) delayReason -> delayReason.rankedList.get(0)._1)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::delayReasonCount, new ProcessingWindowQuery2())
                .keyBy(delayReason -> delayReason.outputDate)
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> multipleIntervalReducer(a, b, 2));

        DataStream<DelayReason> result = outputStreamOperatorInterval1.union(outputStreamOperatorInterval2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::streamsUnion);

        result.print();

        env.execute("Query2");
    }
}
