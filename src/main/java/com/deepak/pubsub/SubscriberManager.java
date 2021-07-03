package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class SubscriberManager {
	private final static SubscriberManager INSTANCE = new SubscriberManager();
	private final Map <String, List <AbstractSubscriber>> channelToSubscribersMap;
	private final Map <AbstractSubscriber, Iterator <QueueManager.MessageBody>> subscriberToQueueMap;

	private SubscriberManager () {
		channelToSubscribersMap = new ConcurrentHashMap <>();
		subscriberToQueueMap = new ConcurrentHashMap <>();
	}

	static SubscriberManager getInstance () {
		return INSTANCE;
	}

	void openChannel (String channel) {
		if (!channelToSubscribersMap.containsKey(channel)) {
			channelToSubscribersMap.put(channel, new ArrayList <>());
		}
	}

	void closeChannel (String channel) {
		List <AbstractSubscriber> subscribers = channelToSubscribersMap.remove(channel);
		for (AbstractSubscriber subscriber : subscribers) {
			subscriberToQueueMap.remove(subscriber);
		}
	}

	int getChannelSize (String channel) throws ChannelDoesNotExistsException {
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		return channelToSubscribersMap.get(channel).size();
	}

	void registerSubscriber (AbstractSubscriber subscriber, Iterator <QueueManager.MessageBody> iterator)
			throws ChannelDoesNotExistsException {
		String channel = subscriber.channel;
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		subscriberToQueueMap.put(subscriber, iterator);
		channelToSubscribersMap.get(channel).add(subscriber);
	}

	void deregisterSubscriber (AbstractSubscriber subscriber) throws ChannelDoesNotExistsException {
		String channel = subscriber.channel;
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		channelToSubscribersMap.get(channel).remove(subscriber);
		subscriberToQueueMap.remove(subscriber);
	}

	Iterator <QueueManager.MessageBody> getSubscriberIterator (AbstractSubscriber subscriber) throws ChannelNotSubscribedException {
		if (!subscriberToQueueMap.containsKey(subscriber)) {
			throw new ChannelNotSubscribedException(subscriber.channel);
		}
		return subscriberToQueueMap.get(subscriber);
	}
}
