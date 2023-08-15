package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

public class WriteListenerDelegate implements WriteListener {
    private final WriteListener delegate;
    private final AtomicLong counter; 
    public WriteListenerDelegate(AtomicLong counter, WriteListener delegate) {
        this.counter = counter;
        this.delegate = delegate;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(Key key) {
        this.counter.decrementAndGet();
        delegate.onSuccess(key);
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        delegate.onFailure(exception);
    }

}
