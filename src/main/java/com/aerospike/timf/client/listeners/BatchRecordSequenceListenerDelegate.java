package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchRecordSequenceListener;

public class BatchRecordSequenceListenerDelegate implements BatchRecordSequenceListener {
    private final BatchRecordSequenceListener delegate;
    private final AtomicLong counter; 
    public BatchRecordSequenceListenerDelegate(AtomicLong counter, BatchRecordSequenceListener delegate) {
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
    public void onRecord(BatchRecord record, int index) throws AerospikeException {
        delegate.onRecord(record, index);
    }
}
