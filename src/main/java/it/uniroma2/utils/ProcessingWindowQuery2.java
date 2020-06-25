package it.uniroma2.utils;

import it.uniroma2.entity.DelayReason;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class ProcessingWindowQuery2 extends ProcessWindowFunction<DelayReason, DelayReason, Object, TimeWindow> {

    @Override
    public void process(Object o, Context context, Iterable<DelayReason> iterable, Collector<DelayReason> collector) {

        String start = String.valueOf(context.window().getStart());
        iterable.iterator().next().setOutputDate(start);
        collector.collect(iterable.iterator().next());
    }
}
