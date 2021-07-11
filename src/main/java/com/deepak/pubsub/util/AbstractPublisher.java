package com.deepak.pubsub.util;

import com.deepak.pubsub.contract.IBroker;
import com.deepak.pubsub.contract.IPublisher;
import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.PartitionLimitException;
import com.deepak.pubsub.exception.QueueOverflowException;
import com.deepak.pubsub.implementation.array.Broker;
import org.json.JSONObject;

public abstract class AbstractPublisher implements IPublisher {
	private final String channel;
	private final IBroker broker;

	protected AbstractPublisher (String channel) {
		this(channel, Broker.getInstance());
	}

	protected AbstractPublisher (String channel, IBroker broker) {
		this.channel = channel;
		this.broker = broker;
	}

	@Override
	public void register () throws PartitionLimitException {
		broker.openChannel(channel);
	}

	@Override
	public void deregister () {
		broker.closeChannel(channel);
	}

	@Override
	public void publish (JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException {
		broker.publish(channel, message);
	}
}
