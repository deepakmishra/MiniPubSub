package com.deepak.pubsub.implementation.queue;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.contract.IBroker;
import com.deepak.pubsub.contract.ISubscriber;
import org.json.JSONObject;

import java.util.List;

public class Broker implements IBroker {
	private final static IBroker INSTANCE = new Broker();
	private final QueueManager queueManager;
	private final SubscriberManager subscriberManager;

	private Broker () {
		queueManager = QueueManager.getInstance();
		subscriberManager = SubscriberManager.getInstance();
	}

	public static IBroker getInstance () {
		return INSTANCE;
	}

	@Override
	public void openChannel (String channel) {
		subscriberManager.openChannel(channel);
		queueManager.openChannel(channel);
	}

	@Override
	public void closeChannel (String channel) {
		queueManager.closeChannel(channel);
		subscriberManager.closeChannel(channel);
	}

	@Override
	public void publish (String channel, JSONObject message) throws ChannelDoesNotExistsException {
		queueManager.pushMessage(message, channel);
	}

	@Override
	public void registerSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		QueueManager.MessageNode pointer = queueManager.getNewPointer(subscriber.getChannel());
		subscriberManager.registerSubscriber(subscriber, pointer);
	}

	@Override
	public void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		subscriberManager.deregisterSubscriber(subscriber);
	}

	@Override
	public JSONObject poll (ISubscriber subscriber) throws ChannelNotSubscribedException {
		return subscriberManager.pollMessage(subscriber);
	}

	@Override
	public List <JSONObject> poll (ISubscriber subscriber, int maxCount) throws ChannelNotSubscribedException {
		return subscriberManager.pollMessages(subscriber, maxCount);
	}

	@Override
	public void markFailed (ISubscriber subscriber, JSONObject message) {
		subscriberManager.addFailed(subscriber, message);
	}

	@Override
	public JSONObject pollFailed (ISubscriber subscriber) {
		return subscriberManager.pollFailed(subscriber);
	}
}
