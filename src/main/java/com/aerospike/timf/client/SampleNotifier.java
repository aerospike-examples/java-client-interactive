package com.aerospike.timf.client;

import com.aerospike.timf.client.recorders.Sample;

/**
 * This is used to notify listeners whenever a new sample has been added
 * @author tfaulkes
 *
 */
public interface SampleNotifier {
    void sampleAdded(Sample sample);
}
