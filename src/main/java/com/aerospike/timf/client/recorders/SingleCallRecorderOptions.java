package com.aerospike.timf.client.recorders;

public class SingleCallRecorderOptions {
    private volatile boolean showObjectSize = false;
    private volatile boolean showBatchDetails = false;
    private volatile boolean showStackTrace = false;

    public boolean isShowBatchDetails() {
        return showBatchDetails;
    }
    public void setShowBatchDetails(boolean showBatchDetails) {
        this.showBatchDetails = showBatchDetails;
    }
    
    public boolean isShowObjectSize() {
        return showObjectSize;
    }
    public void setShowObjectSize(boolean showObjectSize) {
        this.showObjectSize = showObjectSize;
    }
    
    public boolean isShowStackTrace() {
        return showStackTrace;
    }
    
    public void setShowStackTrace(boolean showStackTrace) {
        this.showStackTrace = showStackTrace;
    }
}
