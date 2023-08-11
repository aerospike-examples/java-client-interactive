package com.aerospike.timf.client.recorders;

import java.util.Date;

public class BasicSample extends Sample {

    public BasicSample(long sampleTime) {
        super(sampleTime);
    }

    public String asString(Date startDate, long startTimeInUs) {
        return "";
    }
}
