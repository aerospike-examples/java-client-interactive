package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.timf.client.TimingUtility;

public class DeleteListenerDelegate implements DeleteListener {
    private final DeleteListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public DeleteListenerDelegate(AtomicLong counter, DeleteListener delegate, TimingUtility timer) {
        this.counter = counter;
        this.delegate = delegate;
        this.timer = timer;
        this.counter.incrementAndGet();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        this.counter.decrementAndGet();
        try {
            delegate.onFailure(exception);
        }
        finally {
            this.timer.end(exception);
        }
    }

    @Override
    public void onSuccess(Key key, boolean existed) {
        this.counter.decrementAndGet();
        try {
            delegate.onSuccess(key, existed);
        }
        finally {
            this.timer.end(key);
        }
    }

}
