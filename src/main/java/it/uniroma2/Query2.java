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

        final String hostname;
        final int inputPort, exportPort, numDays;

        try {

            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            inputPort = params.has("inputPort") ? params.getInt("inputPort") : 9092;
            exportPort = params.has("exportPort") ? params.getInt("exportPort") : 9002;
            numDays = params.getInt("numDays");

        } catch (Exception e) {

            System.err.println("Error passing arguments. Please run 'Query1 " +
                    "--hostname <hostname> --inputPort <inputPort> --exportPort <exportPort> --numDays <numDays>', " +
                    "where hostname (localhost by default), inputPort (9092 by default), exportPort (9002 by default)");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, inputPort, "\n");

        DataStream<DelayReason> outputStreamOperator = text

                .map(Utils::csvParsingQuery2)

                //timestamp assigning by event time (to move tumbling window)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DelayReason>() {
                    @Override
                    public long extractAscendingTimestamp(DelayReason delayReason) {
                        return delayReason.getEventTime();
                    }
                });

        //from here the stream is splitted by interval (AM and PM)

        //AM stream
        DataStream<DelayReason> outputStreamOperatorInterval1 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 1)

                //select delay reason as key
                .keyBy((KeySelector<DelayReason, Object>) delayReason -> delayReason.rankedListAM.get(0)._1)

                //set tumbling window with numDays parameter
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))

                //count occurrences of same delay reasons
                .reduce((a, b) -> delayReasonCount(a, b, 1), new ProcessingWindowQuery2())

                //select date as key
                .keyBy(delayReason -> delayReason.outputDate)

                //set tumbling window with numDays parameter
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))

                //reduce entities with same date
                .reduce((a, b) -> dateReducer(a, b, 1));

        //PM stream
        DataStream<DelayReason> outputStreamOperatorInterval2 = outputStreamOperator

                .filter(delayReason -> delayReason.interval == 2)

                //select delay reason as key
                .keyBy((KeySelector<DelayReason, Object>) delayReason -> delayReason.rankedListPM.get(0)._1)

                //set tumbling window with numDays parameter
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))

                //count occurrences of same delay reasons
                .reduce((a, b) -> delayReasonCount(a, b, 2), new ProcessingWindowQuery2())

                //select date as key
                .keyBy(delayReason -> delayReason.outputDate)

                //set tumbling window with numDays parameter
                .window(TumblingEventTimeWindows.of(Time.days(numDays)))

                //reduce entities with same date
                .reduce((a, b) -> dateReducer(a, b, 2));

        //streams union and results formatting
        DataStream<String> result = outputStreamOperatorInterval1.union(outputStreamOperatorInterval2)
                .windowAll(TumblingEventTimeWindows.of(Time.days(numDays)))
                .reduce(Utils::streamsUnion)
                .map(Utils::delaReasonResultMapper);

        //write results to socket
        result.writeToSocket(hostname, exportPort, String::getBytes);

        env.execute("Query2");
    }
}
