package com.aerospike.timf.client.ui;

/**
 * This is used to process any commands passed from custom components in the UI
 * @author tfaulkes
 *
 */
public interface CallbackNotifier {
    String process(String request);
}
