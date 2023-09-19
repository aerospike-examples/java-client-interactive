package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.timf.client.TimingUtility;

public class WriteListenerDelegate implements WriteListener {
    private final WriteListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public WriteListenerDelegate(AtomicLong counter, WriteListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(Key key) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(key);
        }
        finally {
            this.timer.end(key);
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
