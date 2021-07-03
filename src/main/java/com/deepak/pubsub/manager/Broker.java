package com.deepak.pubsub.manager;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.external.ISubscriber;
import org.json.JSONObject;

import java.util.List;

public class Broker {
	private final static Broker INSTANCE = new Broker();
	private final QueueManager queueManager;
	private final SubscriberManager subscriberManager;

	private Broker () {
		queueManager = QueueManager.getInstance();
		subscriberManager = SubscriberManager.getInstance();
	}

	public static Broker getInstance () {
		return INSTANCE;
	}

	public void openChannel (String channel) {
		subscriberManager.openChannel(channel);
		queueManager.openChannel(channel);
	}

	public void closeChannel (String channel) {
		queueManager.closeChannel(channel);
		subscriberManager.closeChannel(channel);
	}

	public void publish (String channel, JSONObject message) throws ChannelDoesNotExistsException {
		queueManager.pushMessage(message, channel);
	}

	public void registerSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		QueueManager.MessageNode pointer = queueManager.getNewPointer(subscriber.getChannel());
		subscriberManager.registerSubscriber(subscriber, pointer);
	}

	public void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		subscriberManager.deregisterSubscriber(subscriber);
	}

	public JSONObject poll (ISubscriber subscriber) throws ChannelNotSubscribedException {
		return subscriberManager.pollMessage(subscriber);
	}

	public List <JSONObject> poll (ISubscriber subscriber, int maxCount) throws ChannelNotSubscribedException {
		return subscriberManager.pollMessages(subscriber, maxCount);
	}

	public void markFailed (ISubscriber subscriber, JSONObject message) {
		subscriberManager.addFailed(subscriber, message);
	}

	public JSONObject pollFailed (ISubscriber subscriber) {
		return subscriberManager.pollFailed(subscriber);
	}
}
