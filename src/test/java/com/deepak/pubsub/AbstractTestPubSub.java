package com.deepak.pubsub;

import com.deepak.pubsub.contract.IBroker;
import com.deepak.pubsub.contract.ICallback;
import com.deepak.pubsub.contract.IPublisher;
import com.deepak.pubsub.contract.ISubscriber;
import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.ChannelNotSubscribedException;
import com.deepak.pubsub.exception.PartitionLimitException;
import com.deepak.pubsub.exception.QueueOverflowException;
import com.deepak.pubsub.util.AbstractPublisher;
import com.deepak.pubsub.util.AbstractSubscriber;
import com.deepak.pubsub.util.Utility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

public abstract class AbstractTestPubSub {

	protected final IBroker broker;

	private ICallback callback;
	private String channel1, channel2;
	private IPublisher publisher1, publisher2, publisher3;
	private ISubscriber subscriber1, subscriber2, subscriber3, subscriber4;

	private ICallback callbackFailing;
	private IPublisher publisherFailing;
	private ISubscriber subscriberFailing1, subscriberFailing2, subscriberPassing;

	private ISubscriber[] subscribersAutoscaling;

	protected AbstractTestPubSub (IBroker broker) {
		this.broker = broker;
	}

	@BeforeEach
	public void setUp () {
		callback = message -> {
			HttpResponse <String> response = Utility.getResponse(message);
			System.out.println(response);
		};
		channel1 = "test_channel1";
		channel2 = "test_channel2";
		publisher1 = new AbstractPublisher(channel1, broker) {
		};
		publisher2 = new AbstractPublisher(channel1, broker) {
		};
		publisher3 = new AbstractPublisher(channel2, broker) {
		};
		subscriber1 = new AbstractSubscriber(channel1, callback, broker) {
		};
		subscriber2 = new AbstractSubscriber(channel1, callback, broker) {
		};
		subscriber3 = new AbstractSubscriber(channel2, callback, broker) {
		};
		subscriber4 = new AbstractSubscriber(channel2, callback, broker) {
		};

		callbackFailing = message -> {
			callback.callback(message);
			throw new Exception();
		};
		publisherFailing = new AbstractPublisher(channel1, broker) {
		};
		subscriberFailing1 = new AbstractSubscriber(channel1, callbackFailing, broker) {
		};
		subscriberFailing2 = new AbstractSubscriber(channel1, callbackFailing, broker) {
		};
		subscriberPassing = new AbstractSubscriber(channel1, callback, broker) {
		};

		subscribersAutoscaling = new ISubscriber[4];
		for (int i = 0; i < 4; i++) {
			subscribersAutoscaling[i] = new AbstractSubscriber(10, channel1, callback, broker) {
			};
		}
	}

	@Test
	public void testHappyFlowPubSub () throws ChannelDoesNotExistsException, ChannelNotSubscribedException, PartitionLimitException, QueueOverflowException {
		publisher1.register();
		publisher2.register();
		publisher3.register();
		publisher1.publish(Utility.getSampleJson(1));
		publisher1.publish(Utility.getSampleJson(2));

		subscriber1.register();
		subscriber1.pollAndExecute();
		assert subscriber1.poll() == null;

		subscriber2.register();

		publisher2.publish(Utility.getSampleJson(3));
		publisher2.publish(Utility.getSampleJson(4));

		assert (int) subscriber1.poll().get("test_id") == 3;
		assert (int) subscriber1.poll().get("test_id") == 4;

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

	@Test
	public void testFailing () throws ChannelDoesNotExistsException, QueueOverflowException, PartitionLimitException {
		publisherFailing.register();
		subscriberFailing1.register();
		subscriberFailing2.register();
		subscriberPassing.register();

		publisherFailing.publish(Utility.getSampleJson(1));
		publisherFailing.publish(Utility.getSampleJson(2));

		subscriberPassing.pollAndExecute();
		subscriberPassing.pollAndExecute();

		subscriberFailing1.pollAndExecute();
		subscriberFailing1.pollAndExecute();

		subscriberFailing2.pollAndExecute();
		subscriberFailing2.pollAndExecute();

		subscriberFailing2.pollFailedAndExecute();

		assert subscriberPassing.pollFailed() == null;

		assert (int) subscriberFailing1.pollFailed().get("test_id") == 1;
		assert (int) subscriberFailing1.pollFailed().get("test_id") == 2;
		assert subscriberFailing1.pollFailed() == null;

		assert (int) subscriberFailing2.pollFailed().get("test_id") == 2;
		assert (int) subscriberFailing2.pollFailed().get("test_id") == 1;
		assert subscriberFailing2.pollFailed() == null;
	}

	@Test
	public void testAutoscaling () throws ChannelDoesNotExistsException, ChannelNotSubscribedException, PartitionLimitException, QueueOverflowException {
		int subscriberCount = subscribersAutoscaling.length;

		publisher1.register();

		for (int i = 0; i < subscriberCount * 3; i++) {
			publisher1.publish(Utility.getSampleJson(i + 1));
		}

		ISubscriber firstSubscriber = subscribersAutoscaling[0];
		firstSubscriber.register();

		for (int i = 0; i < subscriberCount * 3; i++) {
			publisher1.publish(Utility.getSampleJson(i + 1));
		}

		for (int i = 0; i < subscriberCount; i++) {
			assert (int) subscribersAutoscaling[i].poll().get("test_id") == i + 1;
		}

		for (int i = 0; i < subscriberCount; i++) {
			assert (int) firstSubscriber.poll().get("test_id") == subscriberCount + i + 1;
		}

		for (int i = 0; i < subscriberCount; i++) {
			assert (int) subscribersAutoscaling[i].poll().get("test_id") == 2 * subscriberCount + i + 1;
		}

	}
}
