package com.aerospike.timf.client.ui;

import java.util.List;

import com.aerospike.timf.client.recorders.Sample;

public class SamplesResult {
	private List<Sample> aggregatingSamples;
	private List<Sample> individualSamples;
	
	public SamplesResult(List<Sample> aggregatingSamples, List<Sample> individualSamples) {
		super();
		this.aggregatingSamples = aggregatingSamples;
		this.individualSamples = individualSamples;
	}
	public List<Sample> getAggregatingSamples() {
		return aggregatingSamples;
	}
	public List<Sample> getIndividualSamples() {
		return individualSamples;
	}
}
