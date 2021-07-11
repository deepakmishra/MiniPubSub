package com.deepak.pubsub.exception;

public class QueueOverflowException extends Exception {
	public QueueOverflowException (String channel) {
		super(channel + " Queue overflow. Too many messages in a week. Please consume the messages.");
	}
}
