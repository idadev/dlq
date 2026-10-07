package io.github.idird.kafka.dlq;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * Snapshot of a dead letter topic as seen by its consumer group.
 *
 * @param positions next offset the consumer group will read, per partition.
 * @param endOffsets end offset per partition.
 */
public record DltOffsets(Map<TopicPartition, Long> positions, Map<TopicPartition, Long> endOffsets) {

	public DltOffsets {
		positions = Map.copyOf(positions);
		endOffsets = Map.copyOf(endOffsets);
	}

	public static DltOffsets empty() {
		return new DltOffsets(Map.of(), Map.of());
	}

	/**
	 * Number of messages waiting to be reprocessed.
	 * @return the consumer group lag.
	 */
	public long pending() {
		return this.endOffsets.keySet().stream().mapToLong(this::pending).sum();
	}

	public long pending(TopicPartition partition) {
		Long end = this.endOffsets.get(partition);
		if (end == null) {
			return 0;
		}
		return Math.max(0, end - this.positions.getOrDefault(partition, 0L));
	}

}
