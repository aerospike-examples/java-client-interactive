package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.timf.client.TimingUtility;

public class BatchSequenceListenerDelegate implements BatchSequenceListener {
    private final BatchSequenceListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public BatchSequenceListenerDelegate(AtomicLong counter, BatchSequenceListener delegate, TimingUtility timer) {
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
    public void onRecord(BatchRead record) {
        this.delegate.onRecord(record);
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
}
