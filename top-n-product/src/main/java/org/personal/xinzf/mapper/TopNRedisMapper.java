package org.personal.xinzf.mapper;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class TopNRedisMapper implements RedisMapper<String> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET, "TopN");
    }

    @Override
    public String getKeyFromData(String s) {
        return "TopN";
    }

    @Override
    public String getValueFromData(String s) {
        return s;
    }
}
