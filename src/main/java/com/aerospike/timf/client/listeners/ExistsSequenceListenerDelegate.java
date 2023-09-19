package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.timf.client.TimingUtility;

public class ExistsSequenceListenerDelegate implements ExistsSequenceListener {
    private final ExistsSequenceListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public ExistsSequenceListenerDelegate(AtomicLong counter, ExistsSequenceListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess() {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess();
        }
        finally {
            this.timer.end();
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

    @Override
    public void onExists(Key key, boolean exists) {
        delegate.onExists(key, exists);
    }
}
