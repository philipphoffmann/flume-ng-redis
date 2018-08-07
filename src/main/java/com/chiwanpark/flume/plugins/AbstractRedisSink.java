package com.chiwanpark.flume.plugins;

import com.chiwanpark.flume.plugins.handler.RedisMessageHandler;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;

public abstract class AbstractRedisSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRedisSink.class);

  protected BinaryJedis jedis;

  private String redisHost;
  private int redisPort;
  private int redisTimeout;
  private String redisPassword;
  protected RedisMessageHandler messageHandler;
  protected RedisSinkCounter counter;

  protected void connectToRedisSink() {
    // connecting to Redis using a new BinaryJedis object as reusing the same object to reconnect seems to throw exceptions and the message gets sent multiple times!
    // With a new object, the connection failure exception seems to be thrown only once and then the new connection takes over and picks up what was left off in the channel.
    jedis = new BinaryJedis(redisHost, redisPort, redisTimeout);

    if (!"".equals(redisPassword)) {
      jedis.auth(redisPassword);
    }

    jedis.connect();

    LOG.info("Redis Connected. (host: " + redisHost + ", port: " + String.valueOf(redisPort)
              + ", timeout: " + String.valueOf(redisTimeout) + ")");
  }

  @Override
  public synchronized void start() {
    connectToRedisSink();

    counter.start();
    counter.incrementConnectionCreatedCount();
    super.start();
  }

  @Override
  public synchronized void stop() {
    jedis.disconnect();
    counter.stop();
    super.stop();
  }

  @Override
  public void configure(Context context) {
    redisHost = context.getString("redisHost", "localhost");
    redisPort = context.getInteger("redisPort", 6379);
    redisTimeout = context.getInteger("redisTimeout", 2000);
    redisPassword = context.getString("redisPassword", "");

    if(counter == null) {
      counter = new RedisSinkCounter(getName());
    }

    try {
      String charset = context.getString("messageCharset", "utf-8");
      String handlerClassName = context.getString("handler", "com.chiwanpark.flume.plugins.handler.RawHandler");
      @SuppressWarnings("unchecked")
      Class<? extends RedisMessageHandler> clazz = (Class<? extends RedisMessageHandler>) Class.forName(handlerClassName);
      messageHandler = clazz.getDeclaredConstructor(String.class).newInstance(charset);
    } catch (ClassNotFoundException ex) {
      LOG.error("Error while configuring RedisMessageHandler. Exception follows.", ex);
      Throwables.propagate(ex);
    } catch (ClassCastException ex) {
      LOG.error("Handler is not an instance of RedisMessageHandler. Handler must implement RedisMessageHandler.");
      Throwables.propagate(ex);
    } catch (Exception ex) {
      LOG.error("Error configuring RedisSubscribeDrivenSource!", ex);
      Throwables.propagate(ex);
    }
  }
}
