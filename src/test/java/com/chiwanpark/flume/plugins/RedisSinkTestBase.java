package com.chiwanpark.flume.plugins;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisSinkTestBase {
    protected static final Logger logger = LoggerFactory.getLogger(RedisListDrivenSinkTest.class);

    protected Jedis jedis;
    protected Context context = new Context();
    protected Channel channel = new MemoryChannel();
    protected ChannelSelector channelSelector = new ReplicatingChannelSelector();
    protected AbstractSink sink;
    protected String redisHost = System.getProperty("redisHost", "localhost");
}
