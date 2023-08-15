package com.aerospike.timf.client.listeners;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchOperateListListener;

public class BatchOperateListListenerDelegate implements BatchOperateListListener {
    private final BatchOperateListListener delegate;
    private final AtomicLong counter; 
    public BatchOperateListListenerDelegate(AtomicLong counter, BatchOperateListListener delegate) {
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
    public void onSuccess(List<BatchRecord> records, boolean status) {
        this.counter.decrementAndGet();
        delegate.onSuccess(records, status);
    }
}
