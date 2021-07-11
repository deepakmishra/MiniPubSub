package com.deepak.pubsub.util;

import com.deepak.pubsub.contract.IBroker;
import com.deepak.pubsub.contract.ICallback;
import com.deepak.pubsub.contract.ISubscriber;
import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.implementation.array.Broker;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.Random;

public abstract class AbstractSubscriber implements ISubscriber {
	final int id;
	final String channel;
	private final ICallback callback;
	private final IBroker broker;

	/**
	 * This constructor can be used for autoscaling,
	 * so that each consumer of one type share a common channel and pointer
	 *
	 * @param channel  channel to read on
	 * @param callback callback to run
	 */
	protected AbstractSubscriber (String channel, ICallback callback) {
		this(new Random().nextInt(), channel, callback, Broker.getInstance());
	}

	/**
	 * This constructor can be used for autoscaling,
	 * so that each consumer of one type share a common channel and pointer
	 *
	 * @param id       pass the same id for autoscaling
	 * @param channel  channel to read on
	 * @param callback callback to run
	 */
	protected AbstractSubscriber (int id, String channel, ICallback callback) {
		this(id, channel, callback, Broker.getInstance());
	}

	/**
	 * This constructor can be used for autoscaling,
	 * so that each consumer of one type share a common channel and pointer
	 *
	 * @param channel  channel to read on
	 * @param callback callback to run
	 * @param broker   implementation of broker
	 */
	protected AbstractSubscriber (String channel, ICallback callback, IBroker broker) {
		this(new Random().nextInt(), channel, callback, broker);
	}

	/**
	 * This constructor can be used for autoscaling,
	 * so that each consumer of one type share a common channel and pointer
	 *
	 * @param id       pass the same id for autoscaling
	 * @param channel  channel to read on
	 * @param callback callback to run
	 * @param broker   implementation of broker
	 */
	protected AbstractSubscriber (int id, String channel, ICallback callback, IBroker broker) {
		this.id = id;
		this.channel = channel;
		this.callback = callback;
		this.broker = broker;
	}

	@Override
	public void register () throws ChannelDoesNotExistsException {
		broker.registerSubscriber(this);
	}

	@Override
	public void deregister () throws ChannelDoesNotExistsException {
		broker.deregisterSubscriber(this);
	}

	@Override
	public JSONObject poll () throws ChannelNotSubscribedException, ChannelDoesNotExistsException {
		return broker.poll(this);
	}

	@Override
	public List <JSONObject> poll (int count) throws ChannelNotSubscribedException, ChannelDoesNotExistsException {
		return broker.poll(this, count);
	}

	@Override
	public void pollAndExecute () {
		JSONObject message;
		try {
			message = poll();
		} catch (ChannelNotSubscribedException | ChannelDoesNotExistsException e) {
			return;
		}
		int retry = 3;
		while ((retry--) != 0) {
			try {
				callback.callback(message);
				return;
			} catch (Throwable ignored) {

			}
		}
		broker.markFailed(this, message);
	}


	@Override
	public JSONObject pollFailed () {
		return broker.pollFailed(this);
	}

	@Override
	public void pollFailedAndExecute () {
		JSONObject message = broker.pollFailed(this);
		if (message == null) {
			return;
		}
		int retry = 3;
		while ((retry--) != 0) {
			try {
				callback.callback(message);
				return;
			} catch (Throwable ignored) {

			}
		}
		broker.markFailed(this, message);
	}

	@Override
	public void pollAndExecute (int count) {
		List <JSONObject> messages;
		try {
			messages = poll(count);
		} catch (ChannelNotSubscribedException | ChannelDoesNotExistsException e) {
			return;
		}

		for (JSONObject message : messages) {
			int retry = 3;
			while ((retry--) != 0) {
				try {
					callback.callback(message);
					return;
				} catch (Throwable ignored) {

				}
			}
			broker.markFailed(this, message);
		}
	}

	@Override
	public String getChannel () {
		return channel;
	}

	@Override
	public boolean equals (Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AbstractSubscriber that = (AbstractSubscriber) o;
		return id == that.id;
	}

	@Override
	public int hashCode () {
		return Objects.hash(id);
	}
}
