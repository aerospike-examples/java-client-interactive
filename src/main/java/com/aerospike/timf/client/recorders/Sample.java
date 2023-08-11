package com.aerospike.timf.client.recorders;

import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class Sample {

	protected final long sampleTime;
	protected final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    public abstract String asString(Date startDate, long startTimeInUs);

	public Sample(long sampleTime) {
		super();
		this.sampleTime = sampleTime;
	}

	public long getSampleTime() {
		return sampleTime;
	}
}
