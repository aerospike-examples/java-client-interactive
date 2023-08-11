package com.aerospike.timf.client.recorders;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SampleAggregegator {
	private AtomicInteger failed = new AtomicInteger();
	private AtomicInteger succeeded = new AtomicInteger();
	private AtomicLong cumulativeTimeInUs = new AtomicLong();
	private AtomicLong minUs = new AtomicLong(Long.MAX_VALUE);
	private AtomicLong maxUs = new AtomicLong();
	
	public void addSample(boolean wasSuccessful, long timeInUs) {
		if (wasSuccessful) {
			succeeded.incrementAndGet();
		}
		else {
			failed.incrementAndGet();
		}
		cumulativeTimeInUs.addAndGet(timeInUs);
		if (timeInUs < minUs.get()) {
			minUs.set(timeInUs);
		}
		if (timeInUs > maxUs.get()) {
			maxUs.set(timeInUs);
		}
	}
	
	public AggregatedSample getTimedSample() {
		
		return new AggregatedSample(new Date().getTime(), succeeded.getAndSet(0), failed.getAndSet(0), cumulativeTimeInUs.getAndSet(0), minUs.getAndSet(Long.MAX_VALUE), maxUs.getAndSet(0));
	}
}
