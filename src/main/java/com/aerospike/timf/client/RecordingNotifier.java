package com.aerospike.timf.client;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.aerospike.timf.client.recorders.AggregatingRecorder;
import com.aerospike.timf.client.recorders.Sample;
import com.aerospike.timf.client.recorders.SingleCallRecorder;

public class RecordingNotifier implements ITimingNotifier, IMonitorClient {

	private EnumSet<RecordingType> recordingType = EnumSet.noneOf(RecordingType.class);
	private final IRecorder aggregatingRecorder;
	private final SingleCallRecorder singleCallRecorder;
	private final ConsoleNotifier consoleNotifier = new ConsoleNotifier();
	
	public RecordingNotifier(UiOptions options) {
	    this.aggregatingRecorder = new AggregatingRecorder(options.getAggregatingRecorderOptions());
        this.singleCallRecorder = new SingleCallRecorder(options.getSingleCallRecorderOptions());
        
		this.aggregatingRecorder.setTimeToKeepSamples(3600);
		this.singleCallRecorder.setTimeToKeepSamples(3600);
		
		this.aggregatingRecorder.addSampleNotifier(consoleNotifier);
		this.singleCallRecorder.addSampleNotifier(consoleNotifier);
		
		this.setRecordingType(EnumSet.of(RecordingType.AGGREGATE));
	}
	
	public boolean requiresStackTrace() {
	    return this.singleCallRecorder.requiresStackTrace();
	}
	
	public synchronized void setRecordingType(EnumSet<RecordingType> recordingType) {
        aggregatingRecorder.enable(false);
        singleCallRecorder.enable(false);
        if (recordingType.contains(RecordingType.AGGREGATE)) {
			this.singleCallRecorder.clear();
			this.aggregatingRecorder.enable(true);
        }
        if (recordingType.contains(RecordingType.PER_CALL)) {
            this.singleCallRecorder.enable(true);
		}
		this.recordingType = recordingType;
	}
	
	public synchronized EnumSet<RecordingType> getRecordingType() {
		return recordingType;
	}
	
	public void setFilter(Filter filter) {
	    this.aggregatingRecorder.setFilter(filter);
	    this.singleCallRecorder.setFilter(filter);
	}
	public void notify(long timeUs, long submissionTime, long resultsTime, String description, String stackTrace, Object... args) {
		this.notify(timeUs, submissionTime, resultsTime, description, null, null, stackTrace, args);
	}
	public void notify(long timeUs, long submissionTime, long resultsTime, Object result, String description, String stackTrace, Object... args) {
		this.notify(timeUs, submissionTime, resultsTime, description, null, result, stackTrace, args);
	}
	public void notify(long timeUs, long submissionTime, long resultsTime, RuntimeException exception, String description, String stackTrace, Object... args) {
		this.notify(timeUs, submissionTime, resultsTime, description, exception, null, stackTrace, args);
	}
	
	private void notify(long timeUs, long submissionTime, long resultsTime, String description, RuntimeException exception, Object result, String stackTrace, Object... args) {
		if (recordingType.contains(RecordingType.AGGREGATE)) {
			this.aggregatingRecorder.addSample(timeUs, submissionTime, resultsTime, description, exception, result, stackTrace, args);
		}
		if (recordingType.contains(RecordingType.PER_CALL))	 {
			this.singleCallRecorder.addSample(timeUs, submissionTime, resultsTime, description, exception, result, stackTrace, args);
		}
	}
	
	public List<Sample> getAggregatingSamples(long timeSince) {
		return this.aggregatingRecorder.getSamplesSince(timeSince);
	}
	
	public List<Sample> getSingleCallSamples(long timeSince) {
		return this.singleCallRecorder.getSamplesSince(timeSince);
	}
	
	public boolean setEnabled(boolean enabled) {
        if (recordingType.contains(RecordingType.AGGREGATE)) {
            this.aggregatingRecorder.enable(enabled);
        }
        if (recordingType.contains(RecordingType.PER_CALL)) {
            this.singleCallRecorder.enable(enabled);
        }
        return !enabled;
	}

    @Override
    public boolean isEnabled() {
        return this.aggregatingRecorder.isEnabled() || this.singleCallRecorder.isEnabled();
    }

    @Override
    public void setRecordingTypeOptions(RecordingType recordingType, Map<String, String> options) {
        switch (recordingType) {
        case AGGREGATE:
            this.aggregatingRecorder.setOptions(options);
            break;
        case PER_CALL:
            this.singleCallRecorder.setOptions(options);
            break;
        }
    }
}
