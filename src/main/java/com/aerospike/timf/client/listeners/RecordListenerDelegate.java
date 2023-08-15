package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordListener;

public class RecordListenerDelegate implements RecordListener {
    private final RecordListener delegate;
    private final AtomicLong counter; 
    public RecordListenerDelegate(AtomicLong counter, RecordListener delegate) {
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
    public void onSuccess(Key key, Record record) {
        this.counter.decrementAndGet();
        delegate.onSuccess(key, record);
    }

}
