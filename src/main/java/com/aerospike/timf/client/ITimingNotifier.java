package com.aerospike.timf.client;

public interface ITimingNotifier {
	public void notify(long timeUs, long submissionTime, long resultsTime, String description, String stackTrace, Object ... args);
	public void notify(long timeUs, long submissionTime, long resultsTime, Object result, String description, String stackTrace, Object ... args);
	public void notify(long timeUs, long submissionTime, long resultsTime, RuntimeException exception, String description, String stackTrace, Object ... args);
	public boolean requiresStackTrace();
}
