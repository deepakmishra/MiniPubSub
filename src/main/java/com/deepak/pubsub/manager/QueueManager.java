package com.deepak.pubsub.manager;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class QueueManager {
	private final static QueueManager INSTANCE = new QueueManager();
	private final Map <String, QueueTail> channelToTailMap = new ConcurrentHashMap <>();

	private QueueManager () {

	}

	static QueueManager getInstance () {
		return INSTANCE;
	}

	void openChannel (String channel) {
		if (!channelToTailMap.containsKey(channel)) {
			channelToTailMap.put(channel, new QueueTail());
		}
	}

	void closeChannel (String channel) {
		channelToTailMap.remove(channel);
	}

	MessageNode getNewPointer (String channel) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		return channelToTailMap.get(channel).placeholder;
	}

	void pushMessage (JSONObject message, String channel) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		QueueTail tail = channelToTailMap.get(channel);
		tail.resetTail(message);
	}

	private static class QueueTail {
		private MessageNode placeholder;

		private QueueTail () {
			this.placeholder = new MessageNode();
		}

		void resetTail (JSONObject message) {
			placeholder.message = message;
			placeholder.next = new MessageNode();
			placeholder = placeholder.next;
		}
	}

	static class MessageNode {
		JSONObject message;
		MessageNode next;

		private MessageNode () {

		}
	}
}
