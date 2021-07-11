package com.deepak.pubsub.contract;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import org.json.JSONObject;

import java.util.List;

public interface ISubscriber {
	void register () throws ChannelDoesNotExistsException;

	void deregister () throws ChannelDoesNotExistsException;

	JSONObject poll () throws ChannelNotSubscribedException, ChannelDoesNotExistsException;

	List <JSONObject> poll (int count) throws ChannelNotSubscribedException, ChannelDoesNotExistsException;

	void pollAndExecute ();

	JSONObject pollFailed ();

	void pollFailedAndExecute ();

	void pollAndExecute (int count);

	String getChannel ();
}
