package com.deepak.pubsub.exception;

public class ChannelDoesNotExistsException extends Exception {
	public ChannelDoesNotExistsException (String channel) {
		super(channel + " channel does not exists");
	}
}
