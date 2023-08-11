package com.aerospike.timf.client;

public interface ITimingNotifier {
	public void notify(long timeUs, String description, Object ... args);
	public void notify(long timeUs, Object result, String description, Object ... args);
	public void notify(long timeUs, RuntimeException exception, String description, Object ... args);
}
