package com.deepak.pubsub;

import com.deepak.pubsub.implementation.queue.Broker;

public class TestPubSubQueue extends AbstractTestPubSub {
	protected TestPubSubQueue () {
		super(Broker.getInstance());
	}
}
