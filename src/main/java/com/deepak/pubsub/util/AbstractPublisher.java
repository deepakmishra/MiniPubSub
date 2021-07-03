package com.deepak.pubsub.util;

import com.deepak.pubsub.external.IPublisher;
import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.manager.Broker;
import org.json.JSONObject;

public abstract class AbstractPublisher implements IPublisher {
	private final String channel;
	private final Broker broker;

	protected AbstractPublisher (String channel) {
		this.channel = channel;
		this.broker = Broker.getInstance();
	}

	@Override
	public void register () {
		broker.openChannel(channel);
	}

	@Override
	public void deregister () {
		broker.closeChannel(channel);
	}

	@Override
	public void publish (JSONObject message) throws ChannelDoesNotExistsException {
		broker.publish(channel, message);
	}
}
