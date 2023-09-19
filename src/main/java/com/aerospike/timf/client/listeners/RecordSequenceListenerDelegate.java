package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.timf.client.TimingUtility;

public class RecordSequenceListenerDelegate implements RecordSequenceListener {
    private final RecordSequenceListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public RecordSequenceListenerDelegate(AtomicLong counter, RecordSequenceListener delegate, TimingUtility timer) {
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
    public void onRecord(Key key, Record record) throws AerospikeException {
        this.delegate.onRecord(key, record);
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
