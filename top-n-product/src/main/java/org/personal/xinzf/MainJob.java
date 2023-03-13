package org.personal.xinzf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.personal.xinzf.agg.CountAgg;
import org.personal.xinzf.agg.WindowsResultAgg;
import org.personal.xinzf.pojos.ItemViewCount;
import org.personal.xinzf.pojos.UserBehaviour;
import org.personal.xinzf.processes.TopN;
import org.personal.xinzf.sources.KafkaSources;

import java.time.Duration;

/**
 * Hello world!
 *
 */
public class MainJob {
    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO: Write args or read args from etcd
        KafkaSource<String> kafkaSource = KafkaSources.getKafkaSource("", "", "");
        DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<UserBehaviour> userBehaviourDataStream = stream.map(
                new MapFunction<String, UserBehaviour>() {
                    @Override
                    public UserBehaviour map(String s) throws Exception {
                        UserBehaviour userBehaviour = new ObjectMapper().readValue(s, UserBehaviour.class);
                        return userBehaviour;
                    }
                }
        ).filter(
                new FilterFunction<UserBehaviour>() {
                    @Override
                    public boolean filter(UserBehaviour userBehaviour) throws Exception {
                        return userBehaviour != null;
                    }
                }
        ).filter(
                new FilterFunction<UserBehaviour>() {
                    @Override
                    public boolean filter(UserBehaviour userBehaviour) throws Exception {
                        return userBehaviour.getBahavior().equalsIgnoreCase("pv");
                    }
                }
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
        );

        DataStream<String> processingStream = userBehaviourDataStream.keyBy(
                (KeySelector<UserBehaviour, Integer>) data -> data.getItemId()
        ).window(
                SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))
        ).aggregate(
                new CountAgg(), new WindowsResultAgg()
        ).keyBy(
                (KeySelector<ItemViewCount, Long>) data -> data.getWindowEnd()
        ).process(
                new TopN(5)
        );

        processingStream.print();

        env.execute("Top N products");
    }
}
