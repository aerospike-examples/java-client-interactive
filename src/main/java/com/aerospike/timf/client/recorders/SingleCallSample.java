package com.aerospike.timf.client.recorders;

import java.util.Date;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.timf.client.Parameter;
import com.aerospike.timf.client.Utils;

public class SingleCallSample extends Sample {
	private final String functionName;
	private final Parameter[] parameters;
	private final String exceptionMessage;
	private final Object result;
	private final long resultSize;
	private final long executionTimeUs;
	private final boolean showBatchDetails;
	private final String stackTrace;
	private Key[] batchKeys = null;
	private Record[] batchRecords = null;
	private final long threadId;
	private final long activeAsyncCount;
	
	public SingleCallSample(long timeUs, boolean showBatchDetails, String functionName, Parameter[] parameters, String exceptionMessage, Object result, long resultSize, String stackTrace, long threadId, long activeAsyncCount) {
		super(new Date().getTime());
		this.executionTimeUs = timeUs;
		this.functionName = functionName;
		this.parameters = parameters;
		this.exceptionMessage = exceptionMessage;
		this.result = result;
		this.resultSize = resultSize;
		this.showBatchDetails = showBatchDetails;
		this.stackTrace = stackTrace;
		this.threadId = threadId;
		this.activeAsyncCount = activeAsyncCount;
	}

	public String getFunctionName() {
		return functionName;
	}

	public Parameter[] getParameters() {
		return parameters;
	}

	private String getParametersAsString() {
        StringBuffer sb = new StringBuffer().append('(');
        if (parameters != null && parameters.length > 0) {
            for (int i= 0; i < parameters.length; i++) {
                boolean skipSize = false;
                Parameter thisParam = parameters[i];
                sb.append(thisParam.getName()).append(':');
                if (thisParam.getValue() == null) {
                    sb.append("null");
                }
                else if (thisParam.getValue() instanceof Policy) {
                    sb.append(Utils.toString((Policy)thisParam.getValue()));
                }
                // Moved this code to the single call recorder to prevent serializing / sizing listeners and event loops
//                else if (EventLoop.class.isAssignableFrom(thisParam.getValue().getClass())) {
//                    EventLoop loop = (EventLoop)thisParam.getValue();
//                    sb.append("{[").append(loop.getIndex()).append("],ps:").append(loop.getProcessSize()).append(",qs:").append(loop.getQueueSize()).append("}");
//                    skipSize = true;
//                }
//                else if ("listener".equals(thisParam.getName())) {
//                    sb.append("<function>");
//                    skipSize = true;
//                }
                else if (thisParam.getValue().getClass().isArray()) {
                    Class<?> elementType = thisParam.getValue().getClass().getComponentType();
                    Object[] array = (Object[])thisParam.getValue();
                    sb.append(elementType.getSimpleName()).append('[').append(array.length).append(']');
                    if (Key.class.isAssignableFrom(elementType)) {
                        this.batchKeys = (Key[])array;
                    }
                }
                else {
                    sb.append(thisParam.getValue());
                }
                if (thisParam.getSize() > 0 && !skipSize) {
                    sb.append('|').append(Utils.humanReadableByteCountBinary(thisParam.getSize())).append('|');
                }
                sb.append(", ");
            }
            sb.replace(sb.length()-2, sb.length(), "");
        }
        sb.append(')');
        return sb.toString();
	}
	
	private int countNull(Object[] array) {
	    int count = 0;
	    for (int i = 0; i < array.length; i++) {
	        if (array[i] == null) {
	            count++;
	        }
	    }
	    return count;
	}
	private String getResultAsString() {
	    StringBuffer sb = new StringBuffer();
	    if (exceptionMessage != null) {
	        sb.append("Exception(").append(exceptionMessage).append(")");
	    }
	    else if (result == null) {
	        sb.append("none");
	    }
	    else {
	        if (result.getClass().isArray()) {
                Class<?> elementType = result.getClass().getComponentType();
                Object[] array = (Object[])result;
                int nullCount = countNull(array);
                sb.append(elementType.getSimpleName()).append('[').append(array.length);
                if (nullCount > 0) {
                    sb.append('(').append(nullCount).append(" null)");
                }
                sb.append(']');
                if (Record.class.isAssignableFrom(elementType)) {
                    this.batchRecords = (Record[])array;
                }
	        }
	        else {
	            sb.append(result.toString());
	        }
	        if (resultSize > 0) {
	            sb.append('|').append(Utils.humanReadableByteCountBinary(resultSize)).append('|');
	        }
	    }
	    return sb.toString();
	}
	
	public String getBatchDetails() {
	    if (showBatchDetails && batchKeys != null) {
	        StringBuffer sb = new StringBuffer(1000);
	        for (int i = 0; i < batchKeys.length; i++) {
	            sb.append("\n     [").append(i).append("]:").append(batchKeys[i]);
	            if (exceptionMessage == null) {
	                sb.append(": ");
	                if (batchRecords == null) {
	                    sb.append("async call");
	                }
	                else if (i < batchRecords.length) {
	                    Record thisRecord = batchRecords[i];
	                    if (thisRecord == null) {
	                        sb.append("not found");
	                    }
	                    else {
	                        sb.append("found");
	                        if (resultSize > 0) {
	                            // Show individual batch size
	                            sb.append('|').append(Utils.humanReadableByteCountBinary(Utils.estimateSize(thisRecord))).append('|');
	                        }
	                    }
	                }
	            }
	        }
	        return sb.toString();
	    }
	    else {
	        return "";
	    }
	}
	public String getExceptionMessage() {
		return exceptionMessage;
	}

	public long getExecutionTimeUs() {
		return executionTimeUs;
	}
	
	public Object getResult() {
		return result;
	}
	
	public long getResultSize() {
        return resultSize;
    }
	
	public String asString(Date startDate, long startTimeInUs) {
//      System.out.printf("[%,10dus] - %s => %,dus\n", getTimeSinceStartedInUs(), description, timeUs);
	    long startTime = startDate.getTime() + startTimeInUs/1000;
	    String date = sdf.format(new Date(startTime));
	    String stackTrace = this.stackTrace == null ? "" : "\n" + this.stackTrace;
	    // These methods must be called in this order as the first 2 have side effects which affect the next calls. 
	    // Whilst JLS 15.12.4.2 specifies argument order from left to right, it's better to call them explicitly in order.
	    String parametersString = this.getParametersAsString();
	    String resultsString = this.getResultAsString();
	    String batchDetails = getBatchDetails();
        return String.format("[%s]: {%d,%d} %s%s = %s => %,dus%s%s", date, this.threadId, this.activeAsyncCount, this.getFunctionName(),
                parametersString, resultsString, executionTimeUs, batchDetails, stackTrace);
//        return String.format("[%,10dus] - %s%s = %s => %,dus",
//                startTimeInUs, 
//                this.getFunctionName(), 
//                this.getParametersAsString(),
//                this.getResultAsString(),
//                executionTimeUs); 
	}
}
