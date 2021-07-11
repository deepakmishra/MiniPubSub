package com.deepak.pubsub.implementation.array;

import com.deepak.pubsub.exception.ChannelDoesNotExistsException;
import com.deepak.pubsub.exception.PartitionLimitException;
import com.deepak.pubsub.exception.QueueAlreadyInitialisedException;
import com.deepak.pubsub.exception.QueueOverflowException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class QueueManager {
	private final static QueueManager INSTANCE = new QueueManager();
	private static final int PARTITION_ARRAY_SIZE = 10000;
	private final Map <String, AtomicLong> channelToTailMap = new ConcurrentHashMap <>();
	private final Map <String, int[]> channelToPartitionsMap = new ConcurrentHashMap <>();
	private List <QueuePartition> partitions;
	private int partitionCount;
	private List <Integer> allNumbers;

	private QueueManager () {

	}

	static QueueManager getInstance () {
		return INSTANCE;
	}

	void initialise (int partitionCount) throws QueueAlreadyInitialisedException {
		initialise(partitionCount, false);
	}

	void initialise (int partitionCount, boolean force) throws QueueAlreadyInitialisedException {
		if (this.partitions != null && !force) {
			throw new QueueAlreadyInitialisedException();
		}
		this.partitionCount = partitionCount;
		this.partitions = new ArrayList <>(partitionCount);
		for (int i = 0; i < partitionCount; i++) {
			this.partitions.add(new QueuePartition(PARTITION_ARRAY_SIZE));
		}

		this.allNumbers = new ArrayList <>(2 * this.partitionCount);
		for (int i = 0; i < this.partitionCount; i++) {
			this.allNumbers.add(i);
		}
	}

	void openChannel (String channel, int channelPartitionCount) throws PartitionLimitException {
		if (channelToTailMap.containsKey(channel)) {
			return;
		}
		if (channelPartitionCount > this.partitionCount) {
			throw new PartitionLimitException(this.partitionCount);
		}
		for (QueuePartition partition : this.partitions) {
			partition.openChannel(channel);
		}

		Collections.shuffle(this.allNumbers);
		int[] selectedPartitions = this.allNumbers.subList(0, channelPartitionCount).stream().mapToInt(i -> i).toArray();
		channelToPartitionsMap.put(channel, selectedPartitions);

		channelToTailMap.put(channel, new AtomicLong(0));
	}

	void closeChannel (String channel) {
		for (QueuePartition partition : this.partitions) {
			partition.closeChannel(channel);
		}
		channelToPartitionsMap.remove(channel);
		channelToTailMap.remove(channel);
	}

	long getNewPointerIndex (String channel) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		return channelToTailMap.get(channel).get();
	}

	JSONObject getMessage (String channel, long channelIndex) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}

		int[] channelPartitions = channelToPartitionsMap.get(channel);
		int[] partitionAndIndex = getPartitionAndIndex(channelIndex, channelPartitions.length);
		int partitionPosition = partitionAndIndex[0];
		int partitionIndex = partitionAndIndex[1];
		int partition = channelPartitions[partitionPosition];

		QueuePartition queuePartition = this.partitions.get(partition);
		return queuePartition.getMessage(channel, partitionIndex);
	}

	List <JSONObject> getMessages (String channel, long channelIndex, int maxCount) throws ChannelDoesNotExistsException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}
		int[] channelPartitions = channelToPartitionsMap.get(channel);

		Map <Integer, List <Integer>> partitionIndexesMap = new HashMap <>(2 * channelPartitions.length);

		for (int i = 0; i < maxCount; i++) {
			int[] partitionAndIndex = getPartitionAndIndex(channelIndex + i, channelPartitions.length);
			int partitionPosition = partitionAndIndex[0];
			int partitionIndex = partitionAndIndex[1];
			int partition = channelPartitions[partitionPosition];
			if (!partitionIndexesMap.containsKey(partition)) {
				partitionIndexesMap.put(partition, new ArrayList <>());
			}
			partitionIndexesMap.get(partition).add(partitionIndex);
		}

		List <JSONObject> messages = new ArrayList <>();

		for (int partition : channelPartitions) {
			QueuePartition queuePartition = this.partitions.get(partition);
			List <Integer> indexes = partitionIndexesMap.get(partition);
			if (indexes != null) {
				List <JSONObject> messagesFromPartition = queuePartition.getMessages(channel, indexes);
				messages.addAll(messagesFromPartition);
			}
		}

		return messages;
	}

	void pushMessage (String channel, JSONObject message) throws ChannelDoesNotExistsException, QueueOverflowException {
		if (!channelToTailMap.containsKey(channel)) {
			throw new ChannelDoesNotExistsException(channel);
		}

		long channelIndex = channelToTailMap.get(channel).getAndIncrement();

		int[] channelPartitions = channelToPartitionsMap.get(channel);
		int[] partitionAndIndex = getPartitionAndIndex(channelIndex, channelPartitions.length);
		int partitionPosition = partitionAndIndex[0];
		int partitionIndex = partitionAndIndex[1];
		int partition = channelPartitions[partitionPosition];

		QueuePartition queuePartition = this.partitions.get(partition);
		queuePartition.pushMessage(channel, partitionIndex, message);
	}

	int[] getPartitionAndIndex (long channelIndex, int channelPartitionCount) {
		int partition = (int) (channelIndex % channelPartitionCount);
		int partitionIndex = (int) ((channelIndex / channelPartitionCount) % PARTITION_ARRAY_SIZE);
		return new int[]{partition, partitionIndex};
	}


}
