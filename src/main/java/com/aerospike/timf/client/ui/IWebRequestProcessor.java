package com.aerospike.timf.client.ui;

import jakarta.servlet.http.HttpServletRequest;

public interface IWebRequestProcessor {
	/**
	 * Given the details of a web request, return a JSON string representing the payload we want to return to the client.
	 * @param request
	 * @return
	 */
	public String process(HttpServletRequest request);
}
