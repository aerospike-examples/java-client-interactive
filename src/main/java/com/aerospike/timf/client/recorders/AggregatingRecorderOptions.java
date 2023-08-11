package com.aerospike.timf.client.recorders;

public class AggregatingRecorderOptions {
    private int timeToKeepSamplesInSecs = Recorder.DEFAULT_SAMPLE_COUNT;
    
    public AggregatingRecorderOptions() {
    }
    
    public AggregatingRecorderOptions(int timeToKeepSamplesInSecs) {
        super();
        this.timeToKeepSamplesInSecs = timeToKeepSamplesInSecs;
    }

    public int getTimeToKeepSamplesInSecs() {
        return timeToKeepSamplesInSecs;
    }
}
