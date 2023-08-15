package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.DeleteListener;

public class DeleteListenerDelegate implements DeleteListener {
    private final DeleteListener delegate;
    private final AtomicLong counter; 
    public DeleteListenerDelegate(AtomicLong counter, DeleteListener delegate) {
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
    public void onSuccess(Key key, boolean existed) {
        this.counter.decrementAndGet();
        delegate.onSuccess(key, existed);
    }

}
