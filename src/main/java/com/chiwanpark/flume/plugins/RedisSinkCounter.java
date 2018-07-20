package com.chiwanpark.flume.plugins;

import org.apache.flume.instrumentation.SinkCounter;

public class RedisSinkCounter extends SinkCounter implements RedisSinkCounterMBean {
    private static final String COUNTER_SINK_EVENT_SEND_TIME_MICROS = "sink.event.sendTimeMicros";
    private static final String COUNTER_SINK_BATCH_SEND_TIME_MICROS = "sink.batch.sendTimeMicros";
    private static final String COUNTER_SINK_EVENT_ROLLBACK = "sink.event.rollback";
    private static final String COUNTER_SINK_BATCH_ROLLBACK = "sink.batch.rollback";
    private static final String COUNTER_SINK_EVENT_SUCCESS = "sink.event.success";
    private static final String COUNTER_SINK_BATCH_SUCCESS = "sink.batch.success";
    private static final String COUNTER_BATCH_EMPTY = "sink.batch.empty";
    private static final String COUNTER_BATCH_UNDERFLOW = "sink.batch.underflow";
    private static final String COUNTER_BATCH_COMPLETE = "sink.batch.complete";


    private static final String[] ATTRIBUTES = new String[] {
            COUNTER_SINK_EVENT_SEND_TIME_MICROS,
            COUNTER_SINK_BATCH_SEND_TIME_MICROS,
            COUNTER_SINK_EVENT_ROLLBACK,
            COUNTER_SINK_BATCH_ROLLBACK,
            COUNTER_SINK_EVENT_SUCCESS,
            COUNTER_SINK_BATCH_SUCCESS,
            // unfortunately we can't access the final constants in the parent class for the metrics below because they are private
            COUNTER_BATCH_EMPTY,
            COUNTER_BATCH_UNDERFLOW,
            COUNTER_BATCH_COMPLETE
    };

    public RedisSinkCounter(String name) {
        super(name, ATTRIBUTES);
    }

    public void incrementEventSendTimeMicros(long delta) {
        this.addAndGet(COUNTER_SINK_EVENT_SEND_TIME_MICROS, delta);
    }

    public long getEventSendTimeMicros() {
        return this.get(COUNTER_SINK_EVENT_SEND_TIME_MICROS);
    }

    public void incrementBatchSendTimeMicros(long delta) {
        this.addAndGet(COUNTER_SINK_BATCH_SEND_TIME_MICROS, delta);
    }

    public long getBatchSendTimeMicros() {
        return this.get(COUNTER_SINK_BATCH_SEND_TIME_MICROS);
    }

    public void incrementEventRollback() {
        this.increment(COUNTER_SINK_EVENT_ROLLBACK);
    }

    public long getEventRollback() {
        return this.get(COUNTER_SINK_EVENT_ROLLBACK);
    }

    public void incrementBatchRollback() {
        this.increment(COUNTER_SINK_BATCH_ROLLBACK);
    }

    public long getBatchRollback() {
        return this.get(COUNTER_SINK_BATCH_ROLLBACK);
    }

    public void incrementEventSuccess(int processedEvents) {
        for(int i = 0; i < processedEvents; i++) {
            this.increment(COUNTER_SINK_EVENT_SUCCESS);
        }
    }

    public void incrementEventSuccess() {
        incrementEventSuccess(1);
    }

    public long getEventSuccess() {
        return this.get(COUNTER_SINK_EVENT_SUCCESS);
    }

    public void incrementBatchSuccess() {
        this.increment(COUNTER_SINK_BATCH_SUCCESS);
    }

    public long getBatchSuccess() {
        return this.get(COUNTER_SINK_BATCH_SUCCESS);
    }
}
