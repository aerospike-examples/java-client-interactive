package com.aerospike.timf.client.recorders;

import java.util.Date;

public class AggregatedSample extends Sample {
	private final int successfulOps;
	private final int failedOps;
	private final long minTimeUs;
	private final long maxTimeUs;
	private final long avgTimeUs;
	
	public AggregatedSample(long sampleTime, int successfulOps, int failedOps, long cumulativeTimeInUs, long minTimeUs, long maxTimeUs) {
		super(sampleTime);
		long allOps = successfulOps + failedOps;
		this.successfulOps = successfulOps;
		this.failedOps = failedOps;
		this.minTimeUs = (allOps > 0) ? minTimeUs : 0;
		this.maxTimeUs = maxTimeUs;
		this.avgTimeUs = (allOps > 0) ? (cumulativeTimeInUs / allOps) : 0; 
	}

	public int getSuccessfulOps() {
		return successfulOps;
	}

	public int getFailedOps() {
		return failedOps;
	}

	public long getMinTimeUs() {
		return minTimeUs;
	}

	public long getMaxTimeUs() {
		return maxTimeUs;
	}

	public long getAvgTimeUs() {
		return avgTimeUs;
	}

	@Override
	public String toString() {
		if (successfulOps > 0 || failedOps > 0) {
			return String.format("{time=%d, (%d|%d), min:%dus, avg:%dus, max:%dus}", sampleTime, successfulOps, failedOps, minTimeUs, avgTimeUs, maxTimeUs);
		}
		else {
			return String.format("{time=%d, no operations}", sampleTime);
		}
	}

    @Override
    public String asString(Date startDate, long startTimeInUs) {
        return toString();
    }
}
