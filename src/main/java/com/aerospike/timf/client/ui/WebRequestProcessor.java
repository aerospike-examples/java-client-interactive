package com.aerospike.timf.client.ui;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.timf.client.IMonitorClient;
import com.aerospike.timf.client.RecordingNotifier;
import com.aerospike.timf.client.RecordingType;
import com.aerospike.timf.client.recorders.Sample;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletRequest;

public class WebRequestProcessor implements IWebRequestProcessor {
	private final IAerospikeClient client;
	private final RecordingNotifier notifier;
	private final CallbackNotifier callbackHandler;
	private final IMonitorClient monitorClient;
	
	public WebRequestProcessor(IAerospikeClient client, RecordingNotifier notifier, CallbackNotifier callbackHandler, IMonitorClient monitorClient) {
		this.client = client;
		this.notifier = notifier;
		this.callbackHandler = callbackHandler;
		this.monitorClient = monitorClient;
	}
	
	@Override
	public String process(HttpServletRequest request) {
		Map<String, String[]> parameters = request.getParameterMap();
		switch (request.getPathInfo()) {
		case "/nodes": {
			Node[] nodes = client.getNodes();
			String result = "";
			for (Node thisNode : nodes) {
				System.out.println( Info.request(thisNode, "racks:"));
				
				// TODO: Care about racks. But they're done per namespace.
				result += "\"" + thisNode.getName() + "\",";
			}
			return "{ \"nodes\": [" + result +"]}";
		}
		
		case "/samples": {
			long since = 0;
			String[] paramsSince = parameters.get("since");
			if (paramsSince != null && paramsSince.length > 0) {
				since = Integer.parseInt(paramsSince[0]);
			}
			List<Sample> aggregateSamples = notifier.getAggregatingSamples(since);
			List<Sample> individualSamples = notifier.getSingleCallSamples(since);
			SamplesResult samplesResult = new SamplesResult(aggregateSamples, individualSamples);
			ObjectMapper mapper = new ObjectMapper();
			String returnString = null;
			try {
				returnString = mapper.writeValueAsString(samplesResult);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return returnString;
		}
		
		case "/enable": {
		    String[] enabled = parameters.get("enabled");
		    if (enabled != null && enabled.length > 0) {
		        boolean enabledValue = Boolean.valueOf(enabled[0]);
		        boolean previousValue = monitorClient.setEnabled(enabledValue);
		        return String.format("{\"currentEnabled\":%b,\"priorEnabled\":%b}", enabledValue, previousValue);
		    }
		    else {
		        return String.format("{\"error\":,\"Parameter 'enabled' not passed\"}");
		    }
		}
		
		case "/getRecordingType": {
		    EnumSet<RecordingType> types = monitorClient.getRecordingType();
		    StringBuffer result = new StringBuffer(1000);
		    result.append("{\"type\":\"");
		    int count = types.size();
		    for (RecordingType recordingType : types) {
		        result.append(recordingType);
		        if (--count > 0) {
		            result.append(',');
		        }
		    }
		    return result.append("\"}").toString();
		}
		
		case "/recordingType": {
            String[] type = parameters.get("type");
            if (type != null && type.length > 0) {
                String types = type[0];
                String[] typeList = types.split(",");
                EnumSet<RecordingType> typeSet = EnumSet.noneOf(RecordingType.class);
                for (int i = 0; i < typeList.length; i++) {
                    if (!typeList[i].trim().isEmpty()) {
                        RecordingType thisType = RecordingType.valueOf(typeList[i]);
                        typeSet.add(thisType);
                    }
                }
                monitorClient.setRecordingType(typeSet);
                return String.format("{\"recordingType\":\"changed\"}");
            }
            else {
                monitorClient.setRecordingType(EnumSet.noneOf(RecordingType.class));
                return String.format("{\"error\":,\"Parameter 'enabled' not passed\"}");
            }
		}
        case "/typeOptions": {
            String[] type = parameters.get("type");
            if (type != null && type.length > 0) {
                String actualType = type[0];
                RecordingType recordingType = RecordingType.valueOf(actualType);
                String[] optionsList = parameters.get("options");
                Map<String, String> options = new HashMap<>();
                String[] optionsDetails = optionsList[0].split(",");
                for (String thisOptionDetails : optionsDetails) {
                    String[] parts = thisOptionDetails.split(":");
                    options.put(parts[0], parts[1]);
                }
                monitorClient.setRecordingTypeOptions(recordingType, options);
                return String.format("{\"options\":\"changed\"}");
            }
            else {
                return String.format("{\"error\":,\"Parameter 'type' not passed\"}");
            }
        }
		
		case "/callback":
		    String[] params = parameters.get("command");
		    if (this.callbackHandler != null) {
		        return this.callbackHandler.process(params == null || params.length == 0 ? null : params[0]);
		    }
		    else {
		        return "{ \"result\":\"No Command\"}";
		    }
		    
		default:
			String uri = request.getRequestURI();
			
			System.out.println("PathInfo = " + request.getPathInfo());
			System.out.println("uri = " + uri);
			System.out.println("Parameters = " + parameters);
			return "{ \"status\": \"ok\"}";
		}
	}
}
