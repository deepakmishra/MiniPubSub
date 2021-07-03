package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

class QueueManager {
	private final static QueueManager INSTANCE = new QueueManager();
	private final Map <String, Queue <MessageBody>> channelToQueueMap = new ConcurrentHashMap <>();

	private QueueManager () {

	}

	static QueueManager getInstance () {
		return INSTANCE;
	}

	void openChannel (String channel) {
		if (!channelToQueueMap.containsKey(channel)) {
			channelToQueueMap.put(channel, new ConcurrentLinkedQueue <>());
		}
	}

	void closeChannel (String channel) {
		channelToQueueMap.remove(channel);
	}

	Iterator <MessageBody> getNewIterator (String channel) throws ChannelDoesNotExistsException {
		if (!channelToQueueMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		return channelToQueueMap.get(channel).iterator();
	}

	JSONObject pollMessage (Iterator <MessageBody> iterator) {
		if (!iterator.hasNext()) {
			return null;
		}
		MessageBody messageBody = iterator.next();
		messageBody.markRead();
		cleanReadMessagesInQueue(messageBody);
		return messageBody.message;
	}

	List <JSONObject> pollMessages (Iterator <MessageBody> iterator, int count) {
		List <JSONObject> messages = new ArrayList <>();
		MessageBody messageBodyFirst = null;

		while (iterator.hasNext() && messages.size() < count) {
			MessageBody messageBody = iterator.next();
			messageBody.markRead();
			messages.add(messageBody.message);
			if (messageBodyFirst == null) {
				messageBodyFirst = messageBody;
			}
		}

		cleanReadMessagesInQueue(messageBodyFirst);

		return messages;
	}

	void pushMessage (JSONObject message, String channel, int subscriberCount) throws ChannelDoesNotExistsException {
		if (!channelToQueueMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		channelToQueueMap.get(channel).add(new MessageBody(message, channel, subscriberCount));
	}

	private void cleanReadMessagesInQueue (MessageBody messageBody) {
		if (messageBody != null && messageBody.hasAllRead()) {
			Queue <MessageBody> queue = channelToQueueMap.get(messageBody.channel);
			while (true) {
				MessageBody messageBodyToBeRemoved = queue.peek();
				if (messageBodyToBeRemoved == null || !messageBodyToBeRemoved.hasAllRead()
						    || messageBodyToBeRemoved == messageBody) {
					break;
				}
				queue.poll();
			}
		}
	}

	static class Node {
		final JSONObject message;
		final String channel;
		Node next;

		private Node (JSONObject message, String channel) {
			this.message = message;
			this.channel = channel;
		}
	}

	static class MessageBody {
		final JSONObject message;
		final String channel;
		int subscriberCount;

		private MessageBody (JSONObject message, String channel, int subscriberCount) {
			this.message = message;
			this.channel = channel;
			this.subscriberCount = subscriberCount;
		}

		void markRead () {
			this.subscriberCount--;
		}

		boolean hasAllRead () {
			return this.subscriberCount == 0;
		}
	}


}
