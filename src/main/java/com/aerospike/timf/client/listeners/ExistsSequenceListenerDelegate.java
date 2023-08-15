package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsSequenceListener;

public class ExistsSequenceListenerDelegate implements ExistsSequenceListener {
    private final ExistsSequenceListener delegate;
    private final AtomicLong counter; 
    public ExistsSequenceListenerDelegate(AtomicLong counter, ExistsSequenceListener delegate) {
        this.counter = counter;
        this.delegate = delegate;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess() {
        this.counter.decrementAndGet();
        delegate.onSuccess();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        delegate.onFailure(exception);
    }

    @Override
    public void onExists(Key key, boolean exists) {
        delegate.onExists(key, exists);
    }
}
