package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.timf.client.TimingUtility;

public class BatchRecordSequenceListenerDelegate implements BatchRecordSequenceListener {
    private final BatchRecordSequenceListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public BatchRecordSequenceListenerDelegate(AtomicLong counter, BatchRecordSequenceListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onSuccess() {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess();
        }
        finally {
            this.timer.end();
        }
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
    public void onRecord(BatchRecord record, int index) throws AerospikeException {
        delegate.onRecord(record, index);
    }
}
