package it.uniroma2;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import it.uniroma2.utils.ProcessingWindowQuery1;
import it.uniroma2.utils.ProcessingWindowQuery2;
import it.uniroma2.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static it.uniroma2.utils.Utils.*;

@SuppressWarnings("serial")
public class Query1 {

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

            System.err.println("Error passing arguments. Please run 'Query1 " +
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

        DataStream<String> result = text

                .map(line -> csvParsingQuery1(line))

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
                .map(new RichMapFunction<BoroWithDelay, String>() {

                    private transient Meter meter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.meter = getRuntimeContext()
                                .getMetricGroup()
                                .meter("myMeter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
                    }

                    @Override
                    public String map(BoroWithDelay boroWithDelay) throws Exception {
                        this.meter.markEvent();

                        Date outDate = new Date(Long.parseLong(boroWithDelay.getOutputDate()));

                        SimpleDateFormat formatnow = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
                        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd");

                        Date outDateString = null;

                        try {

                            outDateString = formatnow.parse(String.valueOf(outDate));

                        } catch (ParseException e) {

                            e.printStackTrace();
                        }

                        String formattedDate = outFormat.format(outDateString);

                        String[] splittedBoro = boroWithDelay.getBoro().split(",");
                        String[] splittedAverage = boroWithDelay.getAverage().split(",");

                        String result = formattedDate;

                        for (int i = 0; i < splittedBoro.length; i++)
                            result = result + "," + splittedBoro[i] + ":" + splittedAverage[i];

                        return result + "\n";
                    }

                });

        result.writeToSocket(hostname, exportPort, String::getBytes);

        env.execute("Query1");
    }
}

