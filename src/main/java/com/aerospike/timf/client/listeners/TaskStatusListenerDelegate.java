package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.TaskStatusListener;
import com.aerospike.timf.client.TimingUtility;

public class TaskStatusListenerDelegate implements TaskStatusListener {
    private final TaskStatusListener delegate;
    private final AtomicLong counter; 
    private final TimingUtility timer;
    public TaskStatusListenerDelegate(AtomicLong counter, TaskStatusListener delegate, TimingUtility timer) {
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
    public void onSuccess(int status) {
        this.counter.decrementAndGet();
        this.timer.markResultsTime();
        try {
            delegate.onSuccess(status);
        }
        finally {
            this.timer.end(status);
        }
    }

}
