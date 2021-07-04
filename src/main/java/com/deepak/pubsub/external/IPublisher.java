package com.deepak.pubsub.external;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import org.json.JSONObject;

public interface IPublisher {
	void register ();

	void deregister ();

	void publish (JSONObject message) throws ChannelDoesNotExistsException;
}
