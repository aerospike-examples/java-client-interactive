package com.aerospike.timf.client.listeners;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.timf.client.TimingUtility;

public class BatchListListenerDelegate implements BatchListListener {
    private final BatchListListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public BatchListListenerDelegate(AtomicLong counter, BatchListListener delegate, TimingUtility timer) {
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
    public void onSuccess(List<BatchRead> records) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            this.delegate.onSuccess(records);
        }
        finally {
            this.timer.end(records);
        }
    }
}
