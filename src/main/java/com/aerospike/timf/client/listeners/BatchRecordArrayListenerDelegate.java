package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchRecordArrayListener;

public class BatchRecordArrayListenerDelegate implements BatchRecordArrayListener {
    private final BatchRecordArrayListener delegate;
    private final AtomicLong counter; 
    public BatchRecordArrayListenerDelegate(AtomicLong counter, BatchRecordArrayListener delegate) {
        this.counter = counter;
        this.delegate = delegate;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(BatchRecord[] records, boolean status) {
        this.counter.decrementAndGet();
        delegate.onSuccess(records, status);
    }

    @Override
    public void onFailure(BatchRecord[] records, AerospikeException ae) {
        this.counter.decrementAndGet();
        delegate.onFailure(records, ae);
    }
}
