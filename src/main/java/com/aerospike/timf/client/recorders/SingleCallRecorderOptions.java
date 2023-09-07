package com.aerospike.timf.client.recorders;

public class SingleCallRecorderOptions {
    private volatile boolean showResultSize = false;
    private volatile boolean showParameterSizes = false;
    private volatile boolean showBatchDetails = false;
    private volatile boolean showStackTrace = false;

    public boolean isShowBatchDetails() {
        return showBatchDetails;
    }
    public void setShowBatchDetails(boolean showBatchDetails) {
        this.showBatchDetails = showBatchDetails;
    }
    
    public boolean isShowResultSize() {
        return showResultSize;
    }
    
    public void setShowResultSize(boolean showResultSize) {
        this.showResultSize = showResultSize;
    }
    
    public boolean isShowStackTrace() {
        return showStackTrace;
    }
    
    public void setShowStackTrace(boolean showStackTrace) {
        this.showStackTrace = showStackTrace;
    }
    
    public boolean isShowParameterSizes() {
        return showParameterSizes;
    }
    
    public void setShowParameterSizes(boolean showParameterSizes) {
        this.showParameterSizes = showParameterSizes;
    }
}
