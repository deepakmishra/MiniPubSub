package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;

class Broker {
	private final static Broker INSTANCE = new Broker();
	private final QueueManager queueManager;
	private final SubscriberManager subscriberManager;

	private Broker () {
		queueManager = QueueManager.getInstance();
		subscriberManager = SubscriberManager.getInstance();
	}

	static Broker getInstance () {
		return INSTANCE;
	}

	void openChannel (String channel) {
		subscriberManager.openChannel(channel);
		queueManager.openChannel(channel);
	}

	void closeChannel (String channel) {
		queueManager.closeChannel(channel);
		subscriberManager.closeChannel(channel);
	}

	void publish (String channel, JSONObject message) throws ChannelDoesNotExistsException {
		int subscriberCount = subscriberManager.getChannelSize(channel);
		queueManager.pushMessage(message, channel, subscriberCount);
	}

	void registerSubscriber (AbstractSubscriber subscriber) throws ChannelDoesNotExistsException {
		Iterator <QueueManager.MessageBody> iterator = queueManager.getNewIterator(subscriber.channel);
		subscriberManager.registerSubscriber(subscriber, iterator);
	}

	void deregisterSubscriber (AbstractSubscriber subscriber) throws ChannelDoesNotExistsException {
		subscriberManager.deregisterSubscriber(subscriber);
	}

	JSONObject poll (AbstractSubscriber subscriber) throws ChannelNotSubscribedException {
		Iterator <QueueManager.MessageBody> iterator = subscriberManager.getSubscriberIterator(subscriber);
		return queueManager.pollMessage(iterator);
	}

	List <JSONObject> poll (AbstractSubscriber subscriber, int maxCount) throws ChannelNotSubscribedException {
		Iterator <QueueManager.MessageBody> iterator = subscriberManager.getSubscriberIterator(subscriber);
		return queueManager.pollMessages(iterator, maxCount);
	}
}
