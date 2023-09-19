package com.aerospike.timf.client;

import java.util.List;
import java.util.Map;

import com.aerospike.timf.client.recorders.Recorder;
import com.aerospike.timf.client.recorders.Sample;

public interface IRecorder {
	void addSample(long timeUs, long submissionTime, long resultsTime, String description, RuntimeException exception, Object result, String stackTrace, Object... args);
	void clear();
	void reset();
	void enable(boolean isEnabled);
	boolean isEnabled();
	void setFilter(Filter filter);
	void setTimeToKeepSamples(int timeInSecs);
	List<Sample> getSamplesSince(long time);
    Recorder addSampleNotifier(SampleNotifier notifier);
    Recorder removeSampleNotifier(SampleNotifier notifier);
    void setOptions(Map<String, String> options);
}
