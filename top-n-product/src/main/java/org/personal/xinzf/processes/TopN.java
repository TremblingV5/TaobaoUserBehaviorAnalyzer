package org.personal.xinzf.processes;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.personal.xinzf.pojos.ItemViewCount;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
    private int topSize = 5;
    private ListState<ItemViewCount> itemState;

    public TopN(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        List<ItemViewCount> all = new ArrayList<>();
        Iterator<ItemViewCount> iter = itemState.get().iterator();

        while (iter.hasNext()) {
            all.add(iter.next());
        }

        all.sort(
                new Comparator<ItemViewCount>() {
                    @Override
                    public int compare(ItemViewCount o1, ItemViewCount o2) {
                        return o1.getCount() > o2.getCount() ? 1 : 0;
                    }
                }
        );

        List<ItemViewCount> sorted = all.subList(0, topSize);
        itemState.clear();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("current time: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < sorted.size(); i ++) {
            ItemViewCount temp = sorted.get(i);
            stringBuilder.append("No").append(i + 1).append(":").append(" itemId=").append(temp.getItemId()).append(" pv=")
                    .append(temp.getCount()).append("\n");
        }

        TimeUnit.SECONDS.sleep(1);
        out.collect(stringBuilder.toString());
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Long, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
    }
}
