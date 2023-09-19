package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.listener.ClusterStatsListener;
import com.aerospike.timf.client.TimingUtility;

public class ClusterStatsListenerDelegate implements ClusterStatsListener {
    private final ClusterStatsListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public ClusterStatsListenerDelegate(AtomicLong counter, ClusterStatsListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onFailure(exception);
        }
        finally {
            this.timer.end(exception);
        }
    }

    @Override
    public void onSuccess(ClusterStats stats) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(stats);
        }
        finally {
            this.timer.end(stats);
        }
    }
}
