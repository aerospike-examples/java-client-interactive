package com.aerospike.timf.client.listeners;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.InfoListener;

public class InfoListenerDelegate implements InfoListener {
    private final InfoListener delegate;
    private final AtomicLong counter; 
    public InfoListenerDelegate(AtomicLong counter, InfoListener delegate) {
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
    public void onSuccess(Map<String, String> map) {
        this.counter.decrementAndGet();
        delegate.onSuccess(map);
    }

}
