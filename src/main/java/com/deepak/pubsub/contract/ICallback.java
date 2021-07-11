package com.deepak.pubsub.contract;

import org.json.JSONObject;

public interface ICallback {
	void callback (JSONObject message) throws Throwable;
}
