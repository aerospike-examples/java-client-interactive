package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.timf.client.TimingUtility;

public class BatchRecordArrayListenerDelegate implements BatchRecordArrayListener {
    private final BatchRecordArrayListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public BatchRecordArrayListenerDelegate(AtomicLong counter, BatchRecordArrayListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess(BatchRecord[] records, boolean status) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(records, status);
        }
        finally {
            this.timer.end(records);
        }
    }

    @Override
    public void onFailure(BatchRecord[] records, AerospikeException ae) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onFailure(records, ae);
        }
        finally {
            this.timer.end(ae);
        }
    }
}
