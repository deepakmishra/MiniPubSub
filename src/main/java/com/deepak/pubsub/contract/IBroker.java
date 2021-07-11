package com.deepak.pubsub.contract;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.exception.PartitionLimitException;
import com.deepak.pubsub.exception.QueueOverflowException;
import org.json.JSONObject;

import java.util.List;

public interface IBroker {
	void openChannel (String channel) throws PartitionLimitException;

	void closeChannel (String channel);

	void publish (String channel, JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException;

	void registerSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException;

	void deregisterSubscriber (ISubscriber subscriber) throws ChannelDoesNotExistsException;

	JSONObject poll (ISubscriber subscriber) throws ChannelNotSubscribedException, ChannelDoesNotExistsException;

	List <JSONObject> poll (ISubscriber subscriber, int maxCount) throws ChannelNotSubscribedException, ChannelDoesNotExistsException;

	void markFailed (ISubscriber subscriber, JSONObject message);

	JSONObject pollFailed (ISubscriber subscriber);
}
