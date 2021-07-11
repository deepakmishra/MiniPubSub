package com.deepak.pubsub;

import com.deepak.pubsub.implementation.array.Broker;

public class TestPubSubArray extends AbstractTestPubSub {
	protected TestPubSubArray () {
		super(Broker.getInstance());
	}
}
