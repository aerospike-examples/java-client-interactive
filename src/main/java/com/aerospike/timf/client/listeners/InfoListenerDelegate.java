package com.aerospike.timf.client.listeners;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.timf.client.TimingUtility;

public class InfoListenerDelegate implements InfoListener {
    private final InfoListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public InfoListenerDelegate(AtomicLong counter, InfoListener delegate, TimingUtility timer) {
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
    public void onSuccess(Map<String, String> map) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(map);
        }
        finally {
            this.timer.end(map);
        }
    }

}
