package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.timf.client.TimingUtility;

public class IndexListenerDelegate implements IndexListener {
    private final IndexListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public IndexListenerDelegate(AtomicLong counter, IndexListener delegate, TimingUtility timer) {
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
    public void onSuccess(AsyncIndexTask indexTask) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(indexTask);
        }
        finally {
            this.timer.end();
        }
    }

}
