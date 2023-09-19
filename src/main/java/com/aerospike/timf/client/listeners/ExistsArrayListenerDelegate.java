package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.timf.client.TimingUtility;

public class ExistsArrayListenerDelegate implements ExistsArrayListener {
    private final ExistsArrayListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public ExistsArrayListenerDelegate(AtomicLong counter, ExistsArrayListener delegate, TimingUtility timer) {
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
    public void onSuccess(Key[] keys, boolean[] exists) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(keys, exists);
        }
        finally {
            this.timer.end(exists);
        }
    }

}
