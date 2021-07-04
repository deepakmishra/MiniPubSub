package com.deepak.pubsub.util;

import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class Utility {
	private static final String TEST_API = "http://api.test.com";

	public static HttpResponse <String> getResponse (JSONObject payload)
			throws URISyntaxException, IOException, InterruptedException {
		HttpRequest request = HttpRequest.newBuilder(new URI(TEST_API))
				                      .header("Content-Type", "application/json")
				                      .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
				                      .build();

		return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
	}

	public static JSONObject getSampleJson (int testId) {
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("test_id", testId);
		return jsonObject;
	}
}
