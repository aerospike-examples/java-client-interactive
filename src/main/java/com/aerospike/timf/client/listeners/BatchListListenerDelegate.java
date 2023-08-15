package com.aerospike.timf.client.listeners;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchListListener;

public class BatchListListenerDelegate implements BatchListListener {
    private final BatchListListener delegate;
    private final AtomicLong counter; 
    public BatchListListenerDelegate(AtomicLong counter, BatchListListener delegate) {
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
    public void onSuccess(List<BatchRead> records) {
        this.counter.decrementAndGet();
        this.delegate.onSuccess(records);
    }
}
