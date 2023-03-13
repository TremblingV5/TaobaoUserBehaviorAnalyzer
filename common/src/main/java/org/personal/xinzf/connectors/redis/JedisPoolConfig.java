package org.personal.xinzf.connectors.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisPoolConfig extends GenericObjectPoolConfig {
    public JedisPoolConfig() {
        // 开启空闲连接检测
        setTestWhileIdle(true);
        // 空闲连接清除阈值     1分钟
        setMinEvictableIdleTimeMillis(60000);
        // 检测空闲连接的周期    30秒
        setTimeBetweenEvictionRunsMillis(30000);
        // 每次检测时取多少链接检测 -1代表取全部
        setNumTestsPerEvictionRun(-1);

        setBlockWhenExhausted(true);
        setMaxWaitMillis(2000);
    }
}