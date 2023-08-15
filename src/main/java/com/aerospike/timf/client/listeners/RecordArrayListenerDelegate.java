package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordArrayListener;

public class RecordArrayListenerDelegate implements RecordArrayListener {
    private final RecordArrayListener delegate;
    private final AtomicLong counter; 
    public RecordArrayListenerDelegate(AtomicLong counter, RecordArrayListener delegate) {
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
    public void onSuccess(Key[] keys, Record[] records) {
        this.counter.decrementAndGet();
        delegate.onSuccess(keys, records);
    }

}
