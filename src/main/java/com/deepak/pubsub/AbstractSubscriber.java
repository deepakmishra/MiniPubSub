package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.Random;

public abstract class AbstractSubscriber {
	final int id;
	final String channel;
	private final ICallback callback;
	private final Broker broker;

	protected AbstractSubscriber (String channel, ICallback callback) {
		this(new Random().nextInt(), channel, callback);
	}

	/**
	 * This constructor can be used for autoscaling,
	 * so that each consumer of one type share a common channel and pointer
	 *
	 * @param id
	 * @param channel
	 * @param callback
	 */
	protected AbstractSubscriber (int id, String channel, ICallback callback) {
		this.id = id;
		this.channel = channel;
		this.callback = callback;
		this.broker = Broker.getInstance();
	}

	public void register () throws ChannelDoesNotExistsException {
		broker.registerSubscriber(this);
	}

	public void deregister () throws ChannelDoesNotExistsException {
		broker.deregisterSubscriber(this);
	}

	public JSONObject poll () throws ChannelNotSubscribedException {
		return broker.poll(this);
	}

	public List <JSONObject> poll (int count) throws ChannelNotSubscribedException {
		return broker.poll(this, count);
	}

	public void pollAndExecute () {
		JSONObject message = null;
		try {
			message = poll();
		} catch (ChannelNotSubscribedException e) {
			return;
		}
		callback.callback(message);

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
