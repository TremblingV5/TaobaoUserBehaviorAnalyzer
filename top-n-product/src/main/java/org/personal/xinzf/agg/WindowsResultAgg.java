package org.personal.xinzf.agg;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.personal.xinzf.pojos.ItemViewCount;

public class WindowsResultAgg implements WindowFunction<Integer, ItemViewCount, Integer, TimeWindow> {
    @Override
    public void apply(Integer integer, TimeWindow timeWindow, Iterable<Integer> iterable, Collector<ItemViewCount> collector) throws Exception {
        collector.collect(new ItemViewCount(integer, timeWindow.getEnd(), iterable.iterator().next()));
    }
}
