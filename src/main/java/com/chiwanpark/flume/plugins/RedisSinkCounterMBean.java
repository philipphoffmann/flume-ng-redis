package com.chiwanpark.flume.plugins;

public interface RedisSinkCounterMBean {
    public long getEventSendTimeMicros();
    public long getBatchSendTimeMicros();
    public long getEventRollback();
    public long getBatchRollback();
    public long getEventSuccess();
    public long getBatchSuccess();

    // note that we also need to put mbean accessor here which are available already via the base class
    // apparently flume will only pick up the metrics for the mbean accessors found here
    public long getBatchEmptyCount();
    public long getBatchUnderflowCount();
    public long getBatchCompleteCount();
    public long getConnectionCreatedCount();
    public long getConnectionFailedCount();
}
