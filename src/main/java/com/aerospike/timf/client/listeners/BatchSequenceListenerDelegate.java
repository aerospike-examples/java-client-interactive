package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;

public class BatchSequenceListenerDelegate implements BatchSequenceListener {
    private final BatchSequenceListener delegate;
    private final AtomicLong counter; 
    public BatchSequenceListenerDelegate(AtomicLong counter, BatchSequenceListener delegate) {
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
    public void onRecord(BatchRead record) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onSuccess() {
        this.counter.decrementAndGet();
        delegate.onSuccess();
    }
}
