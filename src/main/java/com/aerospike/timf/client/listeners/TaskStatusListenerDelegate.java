package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.listener.TaskStatusListener;

public class TaskStatusListenerDelegate implements TaskStatusListener {
    private final TaskStatusListener delegate;
    private final AtomicLong counter; 
    public TaskStatusListenerDelegate(AtomicLong counter, TaskStatusListener delegate) {
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
    public void onSuccess(int status) {
        this.counter.decrementAndGet();
        delegate.onSuccess(status);
    }

}
