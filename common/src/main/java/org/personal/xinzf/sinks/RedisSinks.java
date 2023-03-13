package org.personal.xinzf.sinks;

import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.personal.xinzf.connectors.redis.OptimizedRedisSink;

public class RedisSinks {

    public static RedisSink getRedisSink(String host, int port, String pwd, RedisMapper mapper) {
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setPassword(pwd)
                //.setMaxIdle(0)      // https://blog.csdn.net/nazeniwaresakini/article/details/104792266
                .build();

        //return new RedisSink<>(conf, mapper);
        return new OptimizedRedisSink<>(conf, mapper);
    }
}