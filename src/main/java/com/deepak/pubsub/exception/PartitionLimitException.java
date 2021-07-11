package com.deepak.pubsub.exception;

public class PartitionLimitException extends Exception {
	public PartitionLimitException (int limit) {
		super("partition size can't be greater than " + limit);
	}
}
