package it.uniroma2;

import it.uniroma2.entity.DelayReason;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

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

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<DelayReason> outputStreamOperator = text

                .map(line -> csvParsingQuery2(line))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DelayReason>() {
                    @Override
                    public long extractAscendingTimestamp(DelayReason delayReason) {
                        return delayReason.getEventTime();
                    }
                })
                .keyBy((KeySelector<DelayReason, Object>) delayReason -> {
                    String delayReasonKey = delayReason.rankedList.get(0)._1;
                    Integer intervalKey = delayReason.interval;

                    return new Tuple2<>(delayReasonKey, intervalKey);                    })
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> delayReasonCount(a, b))
                .keyBy(delayReason -> delayReason.outputDate);

        DataStream<DelayReason> outputStreamOperatorInterval1 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 1)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> multipleIntervalReducer(a, b))
                .keyBy(delayReason -> delayReason.outputDate)
                .reduce((a, b) -> outputIntervalReducer(a, b));

        DataStream<DelayReason> outputStreamOperatorInterval2 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> multipleIntervalReducer(a, b))
                .keyBy(delayReason -> delayReason.outputDate)
                .reduce((a, b) -> outputIntervalReducer(a, b));

        DataStream<DelayReason> result = outputStreamOperatorInterval1.union(outputStreamOperatorInterval2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce((a, b) -> stremsUnion(a, b));

        result.print();

        env.execute("Query2");
    }
}
