package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.listener.ClusterStatsListener;

public class ClusterStatsListenerDelegate implements ClusterStatsListener {
    private final ClusterStatsListener delegate;
    private final AtomicLong counter; 
    public ClusterStatsListenerDelegate(AtomicLong counter, ClusterStatsListener delegate) {
        this.counter = counter;
        this.delegate = delegate;
        this.counter.incrementAndGet();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        delegate.onFailure(exception);
    }

    @Override
    public void onSuccess(ClusterStats stats) {
        this.counter.decrementAndGet();
        delegate.onSuccess(stats);
    }
}
