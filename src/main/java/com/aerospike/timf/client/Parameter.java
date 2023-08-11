package com.aerospike.timf.client;

public class Parameter {
	private final String name;
	private final String type;
	private final Object value;
	private final long size;
	
    public Parameter(final String name, final String type, final Object value) {
        this(name, type, value, 0);
    }
    
	public Parameter(final String name, final String type, final Object value, long size) {
		super();
		this.name = name;
		this.value = value;
		this.type = type;
		this.size = size;
	}
	
	public String getName() {
		return name;
	}
	
	public String getType() {
		return type;
	}
	
	public Object getValue() {
		return value;
	}
	
	public long getSize() {
        return size;
    }
	
	@Override
	public String toString() {
		return String.format("%s:%s=%s", this.name, this.type, this.value);
	}
}
