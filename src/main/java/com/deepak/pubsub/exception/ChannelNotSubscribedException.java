package com.deepak.pubsub.exception;

public class ChannelNotSubscribedException extends Exception {
	public ChannelNotSubscribedException (String channel) {
		super(channel + " is not subscribed");
	}
}
