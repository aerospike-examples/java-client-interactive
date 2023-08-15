package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsListener;

public class ExistsListenerDelegate implements ExistsListener {
    private final ExistsListener delegate;
    private final AtomicLong counter; 
    public ExistsListenerDelegate(AtomicLong counter, ExistsListener delegate) {
        this.counter = counter;
        this.delegate = delegate;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(Key key, boolean exists) {
        this.counter.decrementAndGet();
        delegate.onSuccess(key, exists);
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        delegate.onFailure(exception);
    }
}
