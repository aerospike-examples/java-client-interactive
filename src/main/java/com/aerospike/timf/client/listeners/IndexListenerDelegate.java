package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;
import com.aerospike.client.listener.IndexListener;

public class IndexListenerDelegate implements IndexListener {
    private final IndexListener delegate;
    private final AtomicLong counter; 
    public IndexListenerDelegate(AtomicLong counter, IndexListener delegate) {
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
    public void onSuccess(AsyncIndexTask indexTask) {
        this.counter.decrementAndGet();
        delegate.onSuccess(indexTask);
    }

}
