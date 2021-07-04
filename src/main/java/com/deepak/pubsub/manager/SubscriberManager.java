package com.deepak.pubsub.manager;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.external.ISubscriber;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class SubscriberManager {
	private final static SubscriberManager INSTANCE = new SubscriberManager();
	private final Map <String, List <ISubscriber>> channelToSubscribersMap;
	private final Map <ISubscriber, QueuePointer> subscriberToUnreadPointerMap;
	private final Map <ISubscriber, Queue <JSONObject>> subscriberToRetryMap;

	private SubscriberManager () {
		channelToSubscribersMap = new ConcurrentHashMap <>();
		subscriberToUnreadPointerMap = new ConcurrentHashMap <>();
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
			subscriberToUnreadPointerMap.remove(subscriber);
		}
	}

	void registerSubscriber (ISubscriber subscriber, QueueManager.MessageNode unreadPointer)
			throws ChannelDoesNotExistsException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		subscriberToUnreadPointerMap.put(subscriber, new QueuePointer(unreadPointer));
		channelToSubscribersMap.get(channel).add(subscriber);
	}

	void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException {
		String channel = subscriber.getChannel();
		if (!channelToSubscribersMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		channelToSubscribersMap.get(channel).remove(subscriber);
		subscriberToUnreadPointerMap.remove(subscriber);
	}

	JSONObject pollMessage (ISubscriber subscriber) throws ChannelNotSubscribedException {
		if (!subscriberToUnreadPointerMap.containsKey(subscriber)) {
			throw new ChannelNotSubscribedException(subscriber.getChannel());
		}

		QueuePointer queuePointer = subscriberToUnreadPointerMap.get(subscriber);
		QueueManager.MessageNode unreadPointer = queuePointer.unreadPointer;

		if (unreadPointer == null || unreadPointer.message == null) {
			return null;
		}

		JSONObject message = unreadPointer.message;
		queuePointer.movePointer();
		return message;
	}

	List <JSONObject> pollMessages (ISubscriber subscriber, int count) throws ChannelNotSubscribedException {
		if (!subscriberToUnreadPointerMap.containsKey(subscriber)) {
			throw new ChannelNotSubscribedException(subscriber.getChannel());
		}

		List <JSONObject> messages = new ArrayList <>();

		if (count <= 0) {
			return messages;
		}

		QueuePointer queuePointer = subscriberToUnreadPointerMap.get(subscriber);
		QueueManager.MessageNode unreadPointer = queuePointer.unreadPointer;

		while (unreadPointer != null && unreadPointer.message != null && messages.size() < count) {
			messages.add(unreadPointer.message);
			unreadPointer = queuePointer.movePointer();
		}

		return messages;
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
		return subscriberToRetryMap.get(subscriber).remove();
	}

	private static class QueuePointer {
		private QueueManager.MessageNode unreadPointer;

		private QueuePointer (QueueManager.MessageNode unreadPointer) {
			this.unreadPointer = unreadPointer;
		}

		QueueManager.MessageNode movePointer () {
			QueueManager.MessageNode returnPointer = null;
			if (this.unreadPointer != null && this.unreadPointer.message != null) {
				this.unreadPointer = this.unreadPointer.next;
				returnPointer = this.unreadPointer;
			}
			return returnPointer;
		}
	}

}
