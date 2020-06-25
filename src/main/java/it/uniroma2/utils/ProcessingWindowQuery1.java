package it.uniroma2.utils;

import it.uniroma2.entity.BoroWithDelay;
import it.uniroma2.entity.DelayReason;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessingWindowQuery1 extends ProcessWindowFunction<BoroWithDelay, BoroWithDelay, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<BoroWithDelay> iterable, Collector<BoroWithDelay> collector) {

        String start = String.valueOf(context.window().getStart());
        iterable.iterator().next().setOutputDate(start);
        collector.collect(iterable.iterator().next());
    }
}
