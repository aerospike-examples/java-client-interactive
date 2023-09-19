package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.timf.client.TimingUtility;

public class ExistsListenerDelegate implements ExistsListener {
    private final ExistsListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public ExistsListenerDelegate(AtomicLong counter, ExistsListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(Key key, boolean exists) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(key, exists);
        }
        finally {
            this.timer.end(exists);
        }
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
}
