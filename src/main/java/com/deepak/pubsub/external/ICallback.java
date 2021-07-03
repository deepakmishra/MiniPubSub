package com.deepak.pubsub.external;

import org.json.JSONObject;

public interface ICallback {
	void callback (JSONObject message) throws Throwable;
}
