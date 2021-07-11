package com.deepak.pubsub.exception;

public class QueueAlreadyInitialisedException extends Exception {
	public QueueAlreadyInitialisedException () {
		super("Already initialised. Try force.");
	}
}
