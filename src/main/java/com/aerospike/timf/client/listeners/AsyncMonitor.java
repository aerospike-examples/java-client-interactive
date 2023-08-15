package com.aerospike.timf.client.listeners;

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ClusterStatsListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.TaskStatusListener;
import com.aerospike.client.listener.WriteListener;

public class AsyncMonitor {
    private static AsyncMonitor instance = new AsyncMonitor();
    private volatile boolean enabled = false;
    private AtomicLong activeCount = new AtomicLong();
    
    private AsyncMonitor() {
        super();
    }
    
    public synchronized void setEnabled(boolean enabled) {
        if (enabled && !this.enabled) {
            this.activeCount.set(0);
            this.enabled = true;
        }
        else {
            this.enabled = false;
        }
    }
    
    public static AsyncMonitor getInstance() {
        return instance;
    }
    
    public long getActiveCount() {
        return activeCount.get();
    }
    
    public BatchListListener wrap(BatchListListener delegate) {
        if (enabled) {
            return new BatchListListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public BatchOperateListListener wrap(BatchOperateListListener delegate) {
        if (enabled) {
            return new BatchOperateListListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public BatchRecordArrayListener wrap(BatchRecordArrayListener delegate) {
        if (enabled) {
            return new BatchRecordArrayListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public BatchRecordSequenceListener wrap(BatchRecordSequenceListener delegate) {
        if (enabled) {
            return new BatchRecordSequenceListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public BatchSequenceListener wrap(BatchSequenceListener delegate) {
        if (enabled) {
            return new BatchSequenceListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public ClusterStatsListener wrap(ClusterStatsListener delegate) {
        if (enabled) {
            return new ClusterStatsListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public DeleteListener wrap(DeleteListener delegate) {
        if (enabled) {
            return new DeleteListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public ExecuteListener wrap(ExecuteListener delegate) {
        if (enabled) {
            return new ExecuteListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public ExistsArrayListener wrap(ExistsArrayListener delegate) {
        if (enabled) {
            return new ExistsArrayListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public ExistsListener wrap(ExistsListener delegate) {
        if (enabled) {
            return new ExistsListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public ExistsSequenceListener wrap(ExistsSequenceListener delegate) {
        if (enabled) {
            return new ExistsSequenceListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public IndexListener wrap(IndexListener delegate) {
        if (enabled) {
            return new IndexListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public InfoListener wrap(InfoListener delegate) {
        if (enabled) {
            return new InfoListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public RecordArrayListener wrap(RecordArrayListener delegate) {
        if (enabled) {
            return new RecordArrayListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public RecordListener wrap(RecordListener delegate) {
        if (enabled) {
            return new RecordListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public RecordSequenceListener wrap(RecordSequenceListener delegate) {
        if (enabled) {
            return new RecordSequenceListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public TaskStatusListener wrap(TaskStatusListener delegate) {
        if (enabled) {
            return new TaskStatusListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
    public WriteListener wrap(WriteListener delegate) {
        if (enabled) {
            return new WriteListenerDelegate(activeCount, delegate);
        }
        else {
            return delegate;
        }
    }
}
