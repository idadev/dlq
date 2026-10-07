package io.github.idird.kafka.dlq;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicPartition;

/**
 * State of a reprocessing run. A run only covers the records present in the dead letter topic when it started: the
 * records it puts back are beyond the boundary and wait for the next run.
 */
final class DltRun {

	private final DltReprocessingContext context;

	private final DltRunTrigger trigger;

	private final Clock clock;

	private final Instant startedAt;

	private final Map<TopicPartition, Long> endOffsets;

	private final Set<TopicPartition> completedPartitions = ConcurrentHashMap.newKeySet();

	private final AtomicLong lastActivity;

	private final AtomicBoolean finishing = new AtomicBoolean();

	private final AtomicLong succeeded = new AtomicLong();

	private final AtomicLong failed = new AtomicLong();

	private final AtomicLong parked = new AtomicLong();

	private final AtomicLong skipped = new AtomicLong();

	DltRun(DltReprocessingContext context, DltRunTrigger trigger, DltOffsets offsets, Clock clock) {
		this.context = context;
		this.trigger = trigger;
		this.clock = clock;
		this.startedAt = clock.instant();
		this.lastActivity = new AtomicLong(this.startedAt.toEpochMilli());
		this.endOffsets = offsets.endOffsets();
		this.endOffsets.keySet().stream().filter(partition -> offsets.pending(partition) == 0)
				.forEach(this.completedPartitions::add);
	}

	DltReprocessingContext context() {
		return this.context;
	}

	DltRunTrigger trigger() {
		return this.trigger;
	}

	Instant startedAt() {
		return this.startedAt;
	}

	Instant lastActivity() {
		return Instant.ofEpochMilli(this.lastActivity.get());
	}

	void recordReceived() {
		this.lastActivity.set(this.clock.millis());
	}

	/**
	 * Whether the record was published after the run started.
	 */
	boolean isBeyondBoundary(TopicPartition partition, long offset) {
		Long end = this.endOffsets.get(partition);
		return end != null && offset >= end;
	}

	/**
	 * Whether the record is the last one of its partition covered by the run.
	 */
	boolean isLastBeforeBoundary(TopicPartition partition, long offset) {
		Long end = this.endOffsets.get(partition);
		return end != null && offset + 1 >= end;
	}

	void partitionCompleted(TopicPartition partition) {
		this.completedPartitions.add(partition);
	}

	boolean isCompleteFor(Collection<TopicPartition> assignedPartitions) {
		return assignedPartitions != null && !assignedPartitions.isEmpty()
				&& this.completedPartitions.containsAll(assignedPartitions);
	}

	/**
	 * Claims the right to end the run.
	 * @return false when the run is already ending.
	 */
	boolean beginFinish() {
		return this.finishing.compareAndSet(false, true);
	}

	void succeeded() {
		this.succeeded.incrementAndGet();
	}

	void failed() {
		this.failed.incrementAndGet();
	}

	void parked() {
		this.parked.incrementAndGet();
	}

	void skipped() {
		this.skipped.incrementAndGet();
	}

	DltRunSummary summary(DltRunEndReason reason) {
		return new DltRunSummary(this.context, this.trigger, this.startedAt, this.clock.instant(), reason,
				this.succeeded.get(), this.failed.get(), this.parked.get(), this.skipped.get());
	}

}
