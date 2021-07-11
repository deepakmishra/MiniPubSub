package com.deepak.pubsub.implementation.array;

import com.deepak.pubsub.contract.ISubscriber;
import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class SubscriberManager {
	private final static SubscriberManager INSTANCE = new SubscriberManager();
	private final Map <String, List <ISubscriber>> channelToSubscribersMap;
	private final Map <ISubscriber, AtomicLong> subscriberToUnreadPointerIndexMap;
	private final Map <ISubscriber, Queue <JSONObject>> subscriberToRetryMap;

	private SubscriberManager () {
		channelToSubscribersMap = new ConcurrentHashMap <>();
		subscriberToUnreadPointerIndexMap = new ConcurrentHashMap <>();
		subscriberToRetryMap = new ConcurrentHashMap <>();
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
		List <ISubscriber> subscribers = channelToSubscribersMap.remove(channel);
		for (ISubscriber subscriber : subscribers) {
			subscriberToUnreadPointerIndexMap.remove(subscriber);
		}
	}

	void registerSubscriber (ISubscriber subscriber, long unreadPointerIndex)
			throws ChannelDoesNotExistsException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		subscriberToUnreadPointerIndexMap.put(subscriber, new AtomicLong(unreadPointerIndex));
		channelToSubscribersMap.get(channel).add(subscriber);
	}

	void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		channelToSubscribersMap.get(channel).remove(subscriber);
		subscriberToUnreadPointerIndexMap.remove(subscriber);
	}

	long getSubscriberPointerIndex (ISubscriber subscriber) throws ChannelDoesNotExistsException, ChannelNotSubscribedException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		if (!subscriberToUnreadPointerIndexMap.containsKey(subscriber)) {
			throw new ChannelNotSubscribedException(channel);
		}
		return subscriberToUnreadPointerIndexMap.get(subscriber).get();
	}

	void incrementSubscriberPointerIndex (ISubscriber subscriber, int count) throws ChannelDoesNotExistsException, ChannelNotSubscribedException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		if (!subscriberToUnreadPointerIndexMap.containsKey(subscriber)) {
			throw new ChannelNotSubscribedException(channel);
		}
		subscriberToUnreadPointerIndexMap.get(subscriber).addAndGet(count);
	}

	void addFailed (ISubscriber subscriber, JSONObject message) {
		if (!subscriberToRetryMap.containsKey(subscriber)) {
			subscriberToRetryMap.put(subscriber, new LinkedList <>());
		}
		subscriberToRetryMap.get(subscriber).add(message);
	}

	JSONObject pollFailed (ISubscriber subscriber) {
		if (!subscriberToRetryMap.containsKey(subscriber)) {
			return null;
		}
		return subscriberToRetryMap.get(subscriber).poll();
	}

}
