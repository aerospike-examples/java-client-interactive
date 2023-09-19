package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.timf.client.TimingUtility;

public class ExecuteListenerDelegate implements ExecuteListener {
    private final ExecuteListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public ExecuteListenerDelegate(AtomicLong counter, ExecuteListener delegate, TimingUtility timer) {
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
    public void onSuccess(Key key, Object obj) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(key, obj);
        }
        finally {
            this.timer.end(obj);
        }
    }

}
