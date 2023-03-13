package org.personal.xinzf.connectors.redis;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.container.RedisContainer;
import redis.clients.jedis.JedisPool;

import java.util.Objects;

public class RedisBuilder extends RedisCommandsContainerBuilder {
    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        //if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
        FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig)flinkJedisConfigBase;
        return build(flinkJedisPoolConfig);
        //} else {
        //    throw new IllegalArgumentException("Jedis configuration not found");
        //}
    }

    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        //GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        //genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        //genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        //genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPoolConfig config =  new JedisPoolConfig();
        config.setMaxIdle(jedisPoolConfig.getMaxIdle());
        config.setMaxTotal(jedisPoolConfig.getMaxTotal());
        config.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(
                //genericObjectPoolConfig,
                config,
                jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(),
                jedisPoolConfig.getConnectionTimeout(),
                jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase()
        );
        return new RedisContainer(jedisPool);
    }
}
