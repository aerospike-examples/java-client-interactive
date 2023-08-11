package com.aerospike.timf.client;

import javax.validation.constraints.NotNull;

import com.aerospike.timf.client.recorders.AggregatingRecorderOptions;
import com.aerospike.timf.client.recorders.SingleCallRecorderOptions;
import com.aerospike.timf.client.ui.CallbackNotifier;

public class UiOptions {
    private final int port;
    private final boolean terminateOnClose;
    private final CallbackNotifier callbackNotifier;
    private final AggregatingRecorderOptions aggregatingRecorderOptions;
    private final SingleCallRecorderOptions singleCallRecorderOptions;
    
    public UiOptions(final int port) {
        this(port, true, null);
    }
    
    public UiOptions(final int port, final boolean terminateOnClose) {
        this(port, terminateOnClose, null);
    }
    
    public UiOptions(final int port, final CallbackNotifier callbakNotifier) {
        this(port, true, callbakNotifier);
    }
    
    public UiOptions(final int port, final boolean terminateOnClose, final CallbackNotifier callbackNotifier) {
        this(port, terminateOnClose, callbackNotifier, new AggregatingRecorderOptions(), new SingleCallRecorderOptions());
    }
    
    public UiOptions(final int port, final boolean terminateOnClose, final CallbackNotifier callbackNotifier,
            @NotNull AggregatingRecorderOptions aggregatingRecorderOptions, @NotNull SingleCallRecorderOptions singleCallRecorderOptions) {
        this.port = port;
        this.terminateOnClose = terminateOnClose;
        this.callbackNotifier = callbackNotifier;
        this.aggregatingRecorderOptions = aggregatingRecorderOptions;
        this.singleCallRecorderOptions = singleCallRecorderOptions;
    }
    
    public int getPort() {
        return port;
    }
    
    public CallbackNotifier getCallbackNotifier() {
        return callbackNotifier;
    }
    
    public boolean isTerminateOnClose() {
        return terminateOnClose;
    }
    public AggregatingRecorderOptions getAggregatingRecorderOptions() {
        return aggregatingRecorderOptions;
    }
    public SingleCallRecorderOptions getSingleCallRecorderOptions() {
        return singleCallRecorderOptions;
    }
}
