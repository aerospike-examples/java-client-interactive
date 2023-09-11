package com.aerospike.timf.client.recorders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aerospike.timf.client.Filter;
import com.aerospike.timf.client.IRecorder;

public class AggregatingRecorderOld extends Recorder implements IRecorder {

	private SampleAggregegator aggregator = new SampleAggregegator();
	private int timeToKeepSamplesInSecs = DEFAULT_SAMPLE_COUNT;
//	private long startTimeMs = System.currentTimeMillis();
	private final Thread timerThread;
	
	public AggregatingRecorderOld() {
	    super(false);
		this.sizeData(0, DEFAULT_SAMPLE_COUNT);
		this.timerThread = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
				this.shiftSeconds();
			}
		}, "Aggregating Timer Thread");
		this.timerThread.setDaemon(true);
		this.timerThread.start();
	}
	
	public void addSample(long timeUs, String description, RuntimeException exception, Object result, Object... args) {
		this.aggregator.addSample(exception == null, timeUs);
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}

	private void sizeData(int currentSize, int newSize) {
		if (newSize != currentSize) {
//			this.samples.ensureCapacity(newSize);
			if (newSize > currentSize) {
				// Need to add empty records at the start of the array.
				List<AggregatedSample> emptySamples = new ArrayList<AggregatedSample>(newSize - currentSize);
				for (int i = 0; i < newSize - currentSize; i++) {
					
					// TODO: is this an issue?
					emptySamples.add(new AggregatedSample(0, 0, 0, 0, 0, 0));
				}
				samples.addAll(0, emptySamples);
				// We also need to add time to compensate for these samples. Note that this means moving
				// the start time backwards
//				startTimeMs -= (newSize - currentSize)*1000;
			}
			else {
				// Remove the starting N samples
				samples = samples.subList(currentSize-newSize, currentSize);
//				startTimeMs += (currentSize - newSize)*1000;
			}
		}
	}
	
	public synchronized void setTimeToKeepSamples(int timeInSecs) {
		if (this.timeToKeepSamplesInSecs != timeInSecs) {
			this.sizeData(this.timeToKeepSamplesInSecs, timeInSecs);
			this.timeToKeepSamplesInSecs = timeInSecs;
		}
	}
	
	private synchronized void shiftSeconds() {
//		startTimeMs += 1000;
		samples.remove(0);
		AggregatedSample thisSample = aggregator.getTimedSample();
		samples.add(thisSample);
//		System.out.println(thisSample);
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}

    @Override
    public void enable(boolean isEnabled) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setFilter(Filter filter) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean isEnabled() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setOptions(Map<String, String> options) {
        // TODO Auto-generated method stub
        
    }
}
