package com.aerospike.timf.client.recorders;

import java.util.Map;

import com.aerospike.timf.client.Filter;
import com.aerospike.timf.client.IRecorder;

public class AggregatingRecorder extends Recorder implements IRecorder {

	private SampleAggregegator aggregator = new SampleAggregegator();
//	private long startTimeMs = System.currentTimeMillis();
	private final Thread timerThread;
	private Filter filter;
	private volatile boolean enabled = false;
	private AggregatingRecorderOptions options;
	private int timeToKeepSamplesInSecs;
	
	public AggregatingRecorder(AggregatingRecorderOptions options) {
	    super(false);
	    this.options = options;
	    this.timeToKeepSamplesInSecs = this.options.getTimeToKeepSamplesInSecs();
		this.sizeData(0, this.options.getTimeToKeepSamplesInSecs());
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
	    if (enabled) {
	        this.aggregator.addSample(exception == null, timeUs);
	    }
	}

	public void reset() {
	}

	private void sizeData(int currentSize, int newSize) {
		if (newSize != currentSize) {
			if (newSize < currentSize) {
				// Remove the starting N samples
				samples = samples.subList(currentSize-newSize, currentSize);
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
		if (samples.size() >= timeToKeepSamplesInSecs) {
			samples.remove(0);
		}
		if (enabled) {
    		AggregatedSample thisSample = aggregator.getTimedSample();
    		if (filter == null || filter.apply(thisSample)) {
        		samples.add(thisSample);
                this.sampleAdded(thisSample);
    		}
		}
	}

	public void clear() {
		this.samples.clear();
	}
	
	@Override
	public void enable(boolean isEnabled) {
	    this.enabled = isEnabled;
	}
	
	@Override
	public synchronized void setFilter(Filter filter) {
	    this.filter = filter;
	}
	
	public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setOptions(Map<String, String> options) {
        // TODO Auto-generated method stub
        
    }
}
