package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.ExecuteListener;

public class ExecuteListenerDelegate implements ExecuteListener {
    private final ExecuteListener delegate;
    private final AtomicLong counter; 
    public ExecuteListenerDelegate(AtomicLong counter, ExecuteListener delegate) {
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
    public void onSuccess(Key key, Object obj) {
        this.counter.decrementAndGet();
        delegate.onSuccess(key, obj);
    }

}
