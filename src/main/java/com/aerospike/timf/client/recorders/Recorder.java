package com.aerospike.timf.client.recorders;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import com.aerospike.timf.client.SampleNotifier;

public class Recorder {
	protected static final int DEFAULT_SAMPLE_COUNT = 300;
	private List<SampleNotifier> sampleHanlders = new ArrayList<>();
	private ArrayBlockingQueue<Sample> processQueue = new ArrayBlockingQueue<>(5000);

	private long startTimeNs = System.nanoTime();
	protected List<Sample> samples = new ArrayList<Sample>(DEFAULT_SAMPLE_COUNT);
	private final Thread notifierThread;
	private final Object sampleLock = new Object();
	private final boolean processAsync;
	
	public Recorder(boolean processAsync) {
	    this.processAsync = processAsync;
	    if (processAsync) {
    	    this.notifierThread = new Thread(() -> {
    	        while (true) {
    	            Sample thisSample;
                    try {
                        thisSample = processQueue.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
    	            synchronized (sampleLock) {
        	            for (SampleNotifier handler : sampleHanlders) {
        	                try {
        	                    handler.sampleAdded(thisSample);
        	                }
        	                catch (Exception e) {
        	                    System.out.printf("Exception caught from adding sample: %s (%s)\n", e.getMessage(), e.getCause());
        	                    e.printStackTrace();
        	                }
        	            }
    	            }
    	        }
    	    });
    	    this.notifierThread.setDaemon(true);
    	    this.notifierThread.setName(this.getClass().getSimpleName()+"SampleNotifierThread");
    	    this.notifierThread.start();
	    }
	    else {
	        this.notifierThread = null;
	    }
	}

	public long getTimeSinceStartedInUs() {
		return (System.nanoTime() - startTimeNs) / 1000;
	}

	public long getTimeSinceStartedInSecs() {
		return (System.nanoTime() - startTimeNs) / 1_000_000_000;
	}
	
	public synchronized List<Sample> getSamplesSince(long time) {
		// The samples are in sorted order so we can use binary search to find the first sample
		Sample compareTo = new BasicSample(time);
		int insertPoint = Collections.binarySearch(samples, compareTo, (a, b) -> a.getSampleTime() > b.getSampleTime() ? 1 : a.sampleTime == b.sampleTime ? 0 : -1);
		// We cannot just return the base list as it might be modified by a new sample coming in. 
		if (insertPoint >= 0) {
			return new ArrayList<>(samples.subList(insertPoint, samples.size()));
		}
		else {
			// binarySearch return]s -(insertionPoint)-1 if it doesn't find an exact match.
			insertPoint = -(insertPoint+1);
			if (insertPoint < samples.size()) {
				return new ArrayList<>(samples.subList(insertPoint, samples.size()));
			}
			else {
				return new ArrayList<>();
			}
		}
	}
	
	/**
	 * Add a new handler to the notifier. 
	 * @param notifier
	 * @return
	 */
	public Recorder addSampleNotifier(SampleNotifier notifier) {
        synchronized (sampleLock) {
            this.sampleHanlders.add(notifier);
            return this;
        }
	}
	public synchronized Recorder removeSampleNotifier(SampleNotifier notifier) {
	    synchronized (sampleLock) {
	        this.sampleHanlders.remove(notifier);
	        return this;
        }
	}
	
	protected void sampleAdded(Sample sample) {
        if (processAsync) {
    	    try {
                processQueue.put(sample);
            } catch (InterruptedException ignored) {}
        }
        else {
            for (SampleNotifier handler : sampleHanlders) {
                handler.sampleAdded(sample);
            }
        }
	}
}
