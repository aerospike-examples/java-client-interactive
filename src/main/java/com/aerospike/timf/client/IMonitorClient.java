package com.aerospike.timf.client;

import java.util.EnumSet;
import java.util.Map;

public interface IMonitorClient {
    /**
     * Set the underlying system to be either enabled or disabled depending on the passed value
     * @param enabled
     * @return - the prior value of the enabled flag
     */
    boolean setEnabled(boolean enabled);
    boolean isEnabled();
    
    /**
     * Set the recording type to the passed values
     * @param recordingType
     */
    void setRecordingType(EnumSet<RecordingType> recordingType);
    
    /**
     * Get the list of recorders which are currently active.
     * @return
     */
    EnumSet<RecordingType> getRecordingType();
    
    void setRecordingTypeOptions(RecordingType recordingType, Map<String, String> options);
}
