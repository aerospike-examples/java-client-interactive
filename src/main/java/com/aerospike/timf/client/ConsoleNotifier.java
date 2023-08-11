package com.aerospike.timf.client;

import java.util.Date;

import com.aerospike.timf.client.recorders.Sample;

public class ConsoleNotifier implements SampleNotifier {
    private long startTime = System.nanoTime();
    private Date startDate = new Date();
    
    @Override
    public void sampleAdded(Sample sample) {
        System.out.println(sample.asString(startDate, (System.nanoTime() - startTime)/1000));
    }
}
