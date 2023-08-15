package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExistsArrayListener;

public class ExistsArrayListenerDelegate implements ExistsArrayListener {
    private final ExistsArrayListener delegate;
    private final AtomicLong counter; 
    public ExistsArrayListenerDelegate(AtomicLong counter, ExistsArrayListener delegate) {
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
    public void onSuccess(Key[] keys, boolean[] exists) {
        this.counter.decrementAndGet();
        delegate.onSuccess(keys, exists);
    }

}
