package com.aerospike.timf.client.recorders;

import java.util.Map;

import com.aerospike.timf.client.Filter;
import com.aerospike.timf.client.IRecorder;
import com.aerospike.timf.client.MonitoringAerospikeClient;
import com.aerospike.timf.client.Parameter;
import com.aerospike.timf.client.Utils;

public class SingleCallRecorder extends Recorder implements IRecorder {
    private final SingleCallRecorderOptions options;
    private volatile boolean enabled = false;
    private Filter filter;
    // Specify this as a reference to the class so if the name ever changes we get a compile error
    private static final String FILE_NAME = MonitoringAerospikeClient.class.getSimpleName() + ".java";
    private static final int STACK_TRACE_ELEMENTS_TO_PRINT = 4;
    
    public SingleCallRecorder(SingleCallRecorderOptions options) {
        this.options = options;
    }
	public String getFunctionName(String description) {
		int index = description.indexOf("(");
		if (index > 0) {
			return description.substring(0, index);
		}
		else {
			return description;
		}
	}
	public Parameter[] getParameters(String description, Object[] args) {
		int index = description.indexOf("(");
		int endIndex = description.lastIndexOf(")");
		String params = description.substring(index+1, endIndex);
		String[] paramArray = params.split("\\s*,");
		Parameter[] results = new Parameter[args.length];
		for (int i = 0; i < args.length; i++) {
			if (i < paramArray.length) {
				String thisParamDesc = paramArray[i];
				int paramSeparatorIndex = thisParamDesc.lastIndexOf(' ');
				String type = thisParamDesc.substring(0, paramSeparatorIndex).trim();
				String name = thisParamDesc.substring(paramSeparatorIndex+1).trim();
				long size = (this.options.isShowObjectSize()) ? Utils.estimateSize(args[i]) : 0;
				Parameter param = new Parameter(name, type, args[i], size);
				results[i] = param;
			}
		}
		return results;
	}
	
	private String getStackTrace() {
        String stackTrace = null;
        if (options.isShowStackTrace()) {
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
            boolean found = false;
            StringBuffer sb = new StringBuffer(1000);
            int counter = 0;
            for (int i = 0; i < stackTraceElements.length; i++) {
                if (!found) {
                    // Skip over the parts of the stack which are internal, up to and over the MonitoringAerospikeClient
                    if (FILE_NAME.equals(stackTraceElements[i].getFileName())) {
                        found = true;
                    }
                }
                else {
                    if (counter < STACK_TRACE_ELEMENTS_TO_PRINT) {
                        sb.append("  ").append(stackTraceElements[i].toString()).append('\n');
                        counter++;
                    }
                    else {
                        sb.append("  ... (").append(stackTraceElements.length - i).append(") more.");
                        break;
                    }
                }
            }
            stackTrace = sb.toString();
        }
        return stackTrace;
	}

	public synchronized void addSample(long timeUs, String description, RuntimeException exception, Object result, Object... args) {
	    if (enabled) {
	        if (filter == null || filter.apply(timeUs)) {
                long size = (this.options.isShowObjectSize()) ? Utils.estimateSize(result) : 0;
        		SingleCallSample sample = new SingleCallSample(timeUs, this.options.isShowBatchDetails(), getFunctionName(description), 
        		        getParameters(description, args), exception == null ? null : exception.getMessage(), result, size, getStackTrace());
        		samples.add(sample);
        		if (samples.size() > DEFAULT_SAMPLE_COUNT) {
        			samples.remove(0);
        		}
                this.sampleAdded(sample);
	        }
	    }
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}

	public void setTimeToKeepSamples(int timeInSecs) {
		// TODO Auto-generated method stub
		
	}

	public void clear() {
		// TODO Auto-generated method stub
		
	}
    @Override
    public void enable(boolean isEnabled) {
        this.enabled = isEnabled;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public synchronized void setFilter(Filter filter) {
        this.filter = filter;
    }
    @Override
    public void setOptions(Map<String, String> options) {
        for (String key: options.keySet()) {
            switch (key) {
            case "showSizesControl":
                this.options.setShowObjectSize(Boolean.parseBoolean(options.get(key)));
                break;
            case "showBatchDetails":
                this.options.setShowBatchDetails(Boolean.parseBoolean(options.get(key)));
                break;
            case "showStackTrace":
                this.options.setShowStackTrace(Boolean.parseBoolean(options.get(key)));
                break;
            }
        }
    }

}
