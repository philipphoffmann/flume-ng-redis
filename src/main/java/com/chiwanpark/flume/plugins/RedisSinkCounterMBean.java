package com.chiwanpark.flume.plugins;

public interface RedisSinkCounterMBean {
    public long getSinkSendTimeMicros();
    public long getSinkRollback();
    public long getSinkSuccess();

    // note that we also need to put mbean accessor here which are available already via the base class
    // apparently flume will only pick up the metrics for the mbean accessors found here
    public long getBatchEmptyCount();
    public long getBatchUnderflowCount();
    public long getBatchCompleteCount();
}
