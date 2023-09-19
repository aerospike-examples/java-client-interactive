package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.timf.client.TimingUtility;

public class RecordArrayListenerDelegate implements RecordArrayListener {
    private final RecordArrayListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public RecordArrayListenerDelegate(AtomicLong counter, RecordArrayListener delegate, TimingUtility timer) {
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
    public void onSuccess(Key[] keys, Record[] records) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(keys, records);
        }
        finally {
            this.timer.end(records);
        }
    }

}
