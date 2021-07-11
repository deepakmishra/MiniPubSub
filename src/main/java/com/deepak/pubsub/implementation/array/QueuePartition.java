package com.deepak.pubsub.implementation.array;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.QueueOverflowException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class QueuePartition {
	public static final int EXPIRY_TIME_IN_SECONDS = 3600 * 24 * 7;
	private final Map <String, MessageAndMeta[]> channelToTailMap = new ConcurrentHashMap <>();
	private final int partitionArraySize;

	QueuePartition (int partitionArraySize) {
		this.partitionArraySize = partitionArraySize;
	}

	void openChannel (String channel) {
		if (!channelToTailMap.containsKey(channel)) {
			channelToTailMap.put(channel, new MessageAndMeta[partitionArraySize]);
		}
	}

	void closeChannel (String channel) {
		channelToTailMap.remove(channel);
	}

	JSONObject getMessage (String channel, int index) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}

		MessageAndMeta[] messageArray = channelToTailMap.get(channel);
		if (isExpired(messageArray[index])) {
			return null;
		}
		return messageArray[index].message;
	}

	List <JSONObject> getMessages (String channel, List <Integer> indexes) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}

		MessageAndMeta[] messageArray = channelToTailMap.get(channel);
		List <JSONObject> messages = new ArrayList <>();

		for (int index : indexes) {
			MessageAndMeta messageAndMeta = messageArray[index];
			if (isExpired(messageAndMeta)) {
				return messages;
			}
			messages.add(messageAndMeta.message);
		}
		return messages;
	}

	void pushMessage (String channel, int index, JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		MessageAndMeta[] messageArray = channelToTailMap.get(channel);
		if (!isExpired(messageArray[index])) {
			throw new QueueOverflowException(channel);
		}
		messageArray[index] = new MessageAndMeta(message);
	}

	private boolean isExpired (MessageAndMeta messageAndMeta) {
		if (messageAndMeta == null) {
			return true;
		}
		return new Date().getTime() - messageAndMeta.addedAt.getTime() > 1000L * EXPIRY_TIME_IN_SECONDS;
	}

	private static class MessageAndMeta {
		private final JSONObject message;
		private final Date addedAt;

		private MessageAndMeta (JSONObject message) {
			this.message = message;
			this.addedAt = new Date();
		}
	}
}
