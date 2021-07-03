package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.PublisherException;
import org.json.JSONObject;

public abstract class AbstractPublisher {
	private final String channel;
	private final Broker broker;

	protected AbstractPublisher (String channel) {
		this.channel = channel;
		this.broker = Broker.getInstance();
	}

	public void register() {
		broker.openChannel(channel);
	}

	public void deregister() {
		broker.closeChannel(channel);
	}

	public void publish (JSONObject message) throws ChannelDoesNotExistsException {
		broker.publish(channel, message);
	}
}
