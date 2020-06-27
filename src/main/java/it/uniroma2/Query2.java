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

        // the host and the inputPort to connect to
        final String hostname;
        final int inputPort, exportPort, numDays;

        try {

            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            inputPort = params.getInt("inputPort");
            exportPort = params.getInt("exportPort");
            numDays = params.getInt("numDays");

        } catch (Exception e) {

            System.err.println("Error passing arguments. Please run 'Query2 " +
                    "--hostname <hostname> --inputPort <inputPort> --exportPort <exportPort> --numDays <numDays>', " +
                    "where hostname (localhost by default)");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, inputPort, "\n");

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

        DataStream<String> result = outputStreamOperatorInterval1.union(outputStreamOperatorInterval2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::streamsUnion)
                .map(Utils::delaReasonResultMapper);

        result.writeToSocket(hostname, exportPort, String::getBytes);

        env.execute("Query2");
    }
}
