package com.deepak.pubsub;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.external.ICallback;
import com.deepak.pubsub.external.IPublisher;
import com.deepak.pubsub.external.ISubscriber;
import com.deepak.pubsub.util.AbstractPublisher;
import com.deepak.pubsub.util.AbstractSubscriber;
import com.deepak.pubsub.util.Utility;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;
import java.util.List;

public class TestPubSub {

	ICallback callback;
	String channel1, channel2;
	IPublisher publisher1, publisher2, publisher3;
	ISubscriber subscriber1, subscriber2, subscriber3, subscriber4;

	@BeforeEach
	public void setUp () {
		callback = message -> {
			HttpResponse <String> response = Utility.getResponse(message);
			System.out.println(response);
		};
		channel1 = "test_channel1";
		channel2 = "test_channel2";
		publisher1 = new AbstractPublisher(channel1) {
		};
		publisher2 = new AbstractPublisher(channel1) {
		};
		publisher3 = new AbstractPublisher(channel2) {
		};
		subscriber1 = new AbstractSubscriber(channel1, callback) {
		};
		subscriber2 = new AbstractSubscriber(channel1, callback) {
		};
		subscriber3 = new AbstractSubscriber(channel2, callback) {
		};
		subscriber4 = new AbstractSubscriber(channel2, callback) {
		};
	}


	@Test
	public void testHappyFlowPubSub () throws ChannelDoesNotExistsException, ChannelNotSubscribedException {
		JSONObject message;

		publisher1.register();
		publisher2.register();
		publisher3.register();
		publisher1.publish(Utility.getSampleJson(1));
		publisher1.publish(Utility.getSampleJson(2));

		subscriber1.register();
		subscriber1.pollAndExecute();
		message = subscriber1.poll();
		assert message == null;

		subscriber2.register();

		publisher2.publish(Utility.getSampleJson(3));
		publisher2.publish(Utility.getSampleJson(4));

		message = subscriber1.poll();
		assert (int) message.get("test_id") == 3;

		message = subscriber1.poll();
		assert (int) message.get("test_id") == 4;

		publisher1.publish(Utility.getSampleJson(5));
		publisher1.publish(Utility.getSampleJson(6));

		assert (int) subscriber2.poll().get("test_id") == 3;
		assert (int) subscriber2.poll().get("test_id") == 4;

		assert (int) subscriber2.poll().get("test_id") == 5;
		assert (int) subscriber2.poll().get("test_id") == 6;

		assert (int) subscriber1.poll().get("test_id") == 5;
		assert (int) subscriber1.poll().get("test_id") == 6;

		publisher3.publish(Utility.getSampleJson(7));

		assert subscriber1.poll() == null;
		assert subscriber2.poll() == null;

		subscriber3.register();
		subscriber4.register();

		publisher3.publish(Utility.getSampleJson(8));
		publisher3.publish(Utility.getSampleJson(9));

		assert (int) subscriber3.poll().get("test_id") == 8;
		assert (int) subscriber4.poll().get("test_id") == 8;
		assert (int) subscriber3.poll().get("test_id") == 9;
		assert (int) subscriber4.poll().get("test_id") == 9;

		publisher2.deregister();
		try {
			publisher1.publish(Utility.getSampleJson(10));
			assert false;
		} catch (Exception ignored) {

		}

		try {
			subscriber2.deregister();
			assert false;
		} catch (Exception ignored) {

		}

		try {
			subscriber4.poll();
			assert true;
			subscriber2.poll();
			assert false;
		} catch (Exception ignored) {

		}

		publisher3.publish(Utility.getSampleJson(11));
		publisher3.publish(Utility.getSampleJson(12));

		assert (int) subscriber3.poll().get("test_id") == 11;

		assert subscriber4.poll(3).size() == 2;
		assert subscriber4.poll(3).size() == 0;

		subscriber3.pollAndExecute();
	}
}
