/*
 *  Copyright 2013 Chiwan Park
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@RunWith(JUnit4.class)
public class RedisPublishDrivenSinkTest extends RedisSinkTestBase{
  private static final String TEST_CHANNEL = "flume-ng-redis-test";

  private TestSubscriptionListener listener;
  private Thread thread;

  @Before
  public void setUp() throws Exception {
    context.clear();
    context.put("redisChannel", TEST_CHANNEL);
    context.put("redisHost", redisHost);

    Configurables.configure(channel, context);
    channelSelector.setChannels(Lists.newArrayList(channel));

    sink = new RedisPublishDrivenSink();
    sink.setChannel(channel);
    Configurables.configure(sink, context);

    sink.start();
    channel.start();
    Thread.sleep(1000);

    jedis = new Jedis(redisHost, 6379);
    listener = new TestSubscriptionListener();

    thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          jedis.subscribe(listener, TEST_CHANNEL);
        } catch (JedisConnectionException e) {
          logger.info("Jedis is disconnected.");
        }
      }
    });

    thread.start();
    Thread.sleep(1000);
  }

  @After
  public void tearDown() {
    thread.interrupt();

    channel.stop();
    sink.stop();
    jedis.disconnect();
  }

  @Test
  public void testPublish() throws Exception {
    logger.info("Test Publish feature in RedisPublishDrivenSink");
    String message = "flume-ng-redis-test-message";

    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      logger.info("Put test message into channel");
      channel.put(EventBuilder.withBody(message, Charset.forName("utf-8")));
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
    Assert.assertEquals(message, listener.pollMessage());
  }

  private class TestSubscriptionListener extends JedisPubSub {

    private Queue<String> queue = new ConcurrentLinkedQueue<String>();

    public String pollMessage() {
      String message;

      do {
        message = queue.poll();
      } while (message == null);

      return message;
    }

    @Override
    public void onMessage(String channel, String message) {
      logger.info("Message Arrived!: " + message);

      queue.add(message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
      // ignored
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
      logger.info("Redis Channel " + channel + " is subscribed.");
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
      logger.info("Redis Channel " + channel + " is unsubscribed.");
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
      // ignored
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
      // ignored
    }
  }
}
