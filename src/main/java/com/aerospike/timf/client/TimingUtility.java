package com.aerospike.timf.client;

public class TimingUtility {
	private static ITimingNotifier notifier = null;
	private long startTime;
	private String description;
	private Object[] args;
	
	public TimingUtility start(String description, Object ... args) {
		this.startTime =  System.nanoTime();
		this.description = description;
		this.args = args;
		return this;
	}
	
	public void end() {
		long totalTime = (System.nanoTime() - this.startTime);
		if (TimingUtility.notifier != null) {
			TimingUtility.notifier.notify(totalTime / 1000, description, args);
		}
	}
	
	public <T> T end(T result) {
		long totalTime = (System.nanoTime() - this.startTime);
		if (TimingUtility.notifier != null) {
			TimingUtility.notifier.notify(totalTime / 1000, result, description, args);
		}
		return result;
	}
	
	public RuntimeException end(RuntimeException e) {
		long totalTime = (System.nanoTime() - this.startTime);
		if (TimingUtility.notifier != null) {
			TimingUtility.notifier.notify(totalTime / 1000, e, description, args);
		}
		return e;
	}
	
	public static void setNotifier(ITimingNotifier notifier) {
		TimingUtility.notifier = notifier;
	}
}
