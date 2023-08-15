package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;

public class RecordSequenceListenerDelegate implements RecordSequenceListener {
    private final RecordSequenceListener delegate;
    private final AtomicLong counter; 
    public RecordSequenceListenerDelegate(AtomicLong counter, RecordSequenceListener delegate) {
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
    public void onRecord(Key key, Record record) throws AerospikeException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onSuccess() {
        this.counter.decrementAndGet();
        delegate.onSuccess();
    }

}
