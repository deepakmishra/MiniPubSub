package com.deepak.pubsub.implementation.array;

import com.deepak.pubsub.contract.IBroker;
import com.deepak.pubsub.contract.ISubscriber;
import com.deepak.pubsub.exception.*;
import org.json.JSONObject;

import java.util.List;

public class Broker implements IBroker {
	private final static IBroker INSTANCE = new Broker();
	private final QueueManager queueManager;
	private final SubscriberManager subscriberManager;

	private Broker () {
		subscriberManager = SubscriberManager.getInstance();
		queueManager = QueueManager.getInstance();
		try {
			queueManager.initialise(16);
		} catch (QueueAlreadyInitialisedException e) {
			e.printStackTrace();
		}
	}

	public static IBroker getInstance () {
		return INSTANCE;
	}

	@Override
	public void openChannel (String channel) throws PartitionLimitException {
		subscriberManager.openChannel(channel);
		queueManager.openChannel(channel, 5);
	}

	@Override
	public void closeChannel (String channel) {
		queueManager.closeChannel(channel);
		subscriberManager.closeChannel(channel);
	}

	@Override
	public void publish (String channel, JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException {
		queueManager.pushMessage(channel, message);
	}

	@Override
	public void registerSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		long unreadPointerIndex = queueManager.getNewPointerIndex(subscriber.getChannel());
		subscriberManager.registerSubscriber(subscriber, unreadPointerIndex);
	}

	@Override
	public void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		subscriberManager.deregisterSubscriber(subscriber);
	}

	@Override
	public JSONObject poll (ISubscriber subscriber) throws ChannelNotSubscribedException, ChannelDoesNotExistsException {
		long unreadPointerIndex = subscriberManager.getSubscriberPointerIndex(subscriber);
		JSONObject message = queueManager.getMessage(subscriber.getChannel(), unreadPointerIndex);
		if (message != null) {
			subscriberManager.incrementSubscriberPointerIndex(subscriber, 1);
		}
		return message;
	}

	@Override
	public List <JSONObject> poll (ISubscriber subscriber, int maxCount) throws ChannelNotSubscribedException, ChannelDoesNotExistsException {
		long unreadPointerIndex = subscriberManager.getSubscriberPointerIndex(subscriber);
		List <JSONObject> messages = queueManager.getMessages(subscriber.getChannel(), unreadPointerIndex, maxCount);
		if (messages != null && messages.size() > 0) {
			subscriberManager.incrementSubscriberPointerIndex(subscriber, messages.size());
		}
		return messages;
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
