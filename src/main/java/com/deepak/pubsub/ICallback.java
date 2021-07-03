package com.deepak.pubsub;

import org.json.JSONObject;

public interface ICallback {
	void callback (JSONObject message);
}
