package com.aerospike.timf.client.listeners;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.timf.client.TimingUtility;

public class BatchOperateListListenerDelegate implements BatchOperateListListener {
    private final BatchOperateListListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public BatchOperateListListenerDelegate(AtomicLong counter, BatchOperateListListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onFailure(exception);
        }
        finally {
            this.timer.end(exception);
        }
    }

    @Override
    public void onSuccess(List<BatchRecord> records, boolean status) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(records, status);
        }
        finally {
            this.timer.end(records);
        }
    }
}
