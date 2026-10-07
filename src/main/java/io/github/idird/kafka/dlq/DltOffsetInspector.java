package io.github.idird.kafka.dlq;

/**
 * Reads the progress of a consumer group on a dead letter topic.
 */
@FunctionalInterface
public interface DltOffsetInspector {

	/**
	 * Takes a snapshot of the positions and end offsets.
	 * @param groupId consumer group of the dead letter listener.
	 * @param topic the dead letter topic.
	 * @return the snapshot, empty when the topic does not exist yet.
	 */
	DltOffsets inspect(String groupId, String topic);

}
