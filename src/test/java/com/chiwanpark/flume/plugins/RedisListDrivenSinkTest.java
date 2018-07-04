package com.chiwanpark.flume.plugins;

import com.google.common.collect.Lists;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import java.nio.charset.Charset;

@RunWith(JUnit4.class)
public class RedisListDrivenSinkTest extends RedisSinkTestBase {
  private static final String TEST_LIST = "flume-ng-redis-test";
  private static final String TEST_MESSAGE = "flume-ng-redis-test-message";


  @Before
  public void setUp() {
    context.put("redisList", TEST_LIST);
    context.put("redisHost", redisHost);

    Configurables.configure(channel, context);
    channelSelector.setChannels(Lists.newArrayList(channel));

    sink = new RedisListDrivenSink();
    sink.setChannel(channel);

    jedis = new Jedis(redisHost, 6379);
  }

  @After
  public void tearDown()  {
    jedis.disconnect();
    channel.stop();
    sink.stop();
  }

  @Test
  public void testPushEvent() throws Exception {
    logger.info("Test Publish feature in RedisListDrivenSink");

    Configurables.configure(sink, context);
    sink.start();
    channel.start();

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      logger.info("Put test message into channel");
      channel.put(EventBuilder.withBody(TEST_MESSAGE, Charset.forName("utf-8")));
      transaction.commit();
    } catch (Throwable e) {
      logger.info("Rollback");
      transaction.rollback();
    } finally {
      logger.info("Transaction is closed");
      transaction.close();
    }

    Thread.sleep(1000);
    sink.process();

    Thread.sleep(1000);
    Assert.assertEquals(TEST_MESSAGE, jedis.rpop(TEST_LIST));
  }

  @Test
  public void testSelectDatabase() throws Exception {
    context.put("redisDatabase", "1");

    Configurables.configure(sink, context);
    sink.start();
    channel.start();

    Assert.assertEquals("OK", jedis.select(1));

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      logger.info("Put test message into channel");
      channel.put(EventBuilder.withBody(TEST_MESSAGE, Charset.forName("utf-8")));
      transaction.commit();
    } catch (Throwable e) {
      logger.info("Rollback");
      transaction.rollback();
    } finally {
      logger.info("Transaction is closed");
      transaction.close();
    }

    Thread.sleep(1000);
    sink.process();

    Thread.sleep(1000);
    Assert.assertEquals(TEST_MESSAGE, jedis.rpop(TEST_LIST));
  }
}
