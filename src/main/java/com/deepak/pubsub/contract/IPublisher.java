package com.deepak.pubsub.contract;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.PartitionLimitException;
import com.deepak.pubsub.exception.QueueOverflowException;
import org.json.JSONObject;

public interface IPublisher {
	void register () throws PartitionLimitException;

	void deregister ();

	void publish (JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException;
}
