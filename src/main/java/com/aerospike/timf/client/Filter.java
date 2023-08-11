package com.aerospike.timf.client;

import com.aerospike.timf.client.recorders.Sample;

public class Filter {
    private long minTimeInUs = 0;
    public Filter() {
    }
    
    public Filter minTimeInUs(long minTimeInUs) {
        this.minTimeInUs = minTimeInUs;
        return this;
    }
    
    public boolean apply(Sample sample) {
        return (sample.getSampleTime() >= minTimeInUs);
    }
    public boolean apply(long timeInUs) {
        return timeInUs >= minTimeInUs;
    }
}
