package com.chiwanpark.flume.plugins;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisListDrivenSink extends AbstractRedisSink {
  private static final Logger LOG = LoggerFactory.getLogger(RedisListDrivenSink.class);

  private int redisDatabase;
  private byte[] redisList;
  private int batchSize;

  @Override
  public void configure(Context context) {
    redisDatabase = context.getInteger("redisDatabase", 0);
    redisList = context.getString("redisList").getBytes();
    batchSize = context.getInteger("batchSize", 1);

    Preconditions.checkNotNull(redisList, "Redis List must be set.");

    super.configure(context);
    LOG.info("Flume Redis List Sink Configured");
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  @Override
  public synchronized void start() {
    super.start();

    if (redisDatabase != 0) {
      final String result = jedis.select(redisDatabase);
      if (!"OK".equals(result)) {
        throw new RuntimeException("Cannot select database (database: " + redisDatabase + ")");
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    long startTime = System.nanoTime();

    try {
      transaction.begin();

      // try to connect in case connection to redis sink is lost
      if (!jedis.isConnected()) {
        counter.incrementConnectionFailedCount();
        LOG.info("Connection appears to be lost to Redis sink, attempting to reconnect...");
        LOG.info("Total connection attempts so far = " + counter.getConnectionFailedCount());
        connectToRedisSink();
        counter.incrementConnectionCreatedCount();
      }

      Pipeline pipeline = jedis.pipelined();

      int processedEvents = 0;
      for(; processedEvents < batchSize; processedEvents++) {
        Event event = channel.take();
        if (event == null) {
          // channel is empty
          if (processedEvents == 0) {
            counter.incrementBatchEmptyCount();
          } else {
            // channel has less events than batchSize
            counter.incrementBatchUnderflowCount();
          }
          break;
        }
        byte[] serialized = messageHandler.getBytes(event);

        try {
          pipeline.lpush(redisList, serialized);
        } catch(JedisDataException e) {
          throw new EventDeliveryException("Event cannot be pushed into list " + redisList + " due to an error in response " + e.getMessage());
        }
      }

      // channel has enough events for batchSize
      if (processedEvents == batchSize) {
        counter.incrementBatchCompleteCount();
      }

      pipeline.sync();
      transaction.commit();
      status = Status.READY;

      counter.incrementBatchSuccess();
      counter.incrementEventSuccess(processedEvents);
    } catch (Throwable e) {
      transaction.rollback();
      counter.incrementBatchRollback();
      status = Status.BACKOFF;

      if (e instanceof JedisException) {
        // we need to rethrow jedis exceptions, because they signal that something went wrong
        // with the connection to the redis server
        jedis.disconnect();
        throw new EventDeliveryException(e);
      }

      if (e instanceof Error) {
        throw (Error) e;
      }
    } finally {
      transaction.close();
      counter.incrementBatchSendTimeMicros((System.nanoTime() - startTime) / 1000);
    }

    return status;
  }
}
