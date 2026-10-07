package io.github.idird.kafka.dlq;

import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.SeekUtils;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.listener.adapter.AbstractDelegatingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Wraps the listener of a dead letter container to keep a reprocessing run under control:
 * <ul>
 * <li>records published after the run started are left in the topic for the next run;</li>
 * <li>records rejected by the {@link DltRecordFilter} are put back in the dead letter topic;</li>
 * <li>records the {@code @DltHandler} fails on are put back in the dead letter topic or, once their attempts are
 * exhausted, moved to the parking lot topic.</li>
 * </ul>
 * The handler exception is not propagated once the record is safely re-published, so the {@code DltStrategy} of the
 * {@code @RetryableTopic} is not involved. When the container runs outside a run, the adapter is transparent.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
final class DltReprocessingListenerAdapter<K, V> extends AbstractDelegatingMessageListenerAdapter<MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private final ManagedDlt dlt;

	private final DltRecordPublisher publisher;

	private final DltRecordFilter filter;

	private final Consumer<DltRun> runCompleted;

	DltReprocessingListenerAdapter(MessageListener<K, V> delegate, ManagedDlt dlt, DltRecordPublisher publisher,
			DltRecordFilter filter, Consumer<DltRun> runCompleted) {

		super(delegate);
		this.dlt = dlt;
		this.publisher = publisher;
		this.filter = filter;
		this.runCompleted = runCompleted;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {

		DltRun run = this.dlt.currentRun();
		if (run == null) {
			invokeDelegate(record, acknowledgment, consumer);
			return;
		}
		TopicPartition partition = new TopicPartition(record.topic(), record.partition());
		if (run.isBeyondBoundary(partition, record.offset())) {
			leaveForNextRun(run, partition);
		}
		run.recordReceived();
		process(run, record, acknowledgment, consumer);
		if (run.isLastBeforeBoundary(partition, record.offset())) {
			partitionCompleted(run, partition);
		}
	}

	private void leaveForNextRun(DltRun run, TopicPartition partition) {
		this.dlt.container().pausePartition(partition);
		partitionCompleted(run, partition);
		// handled by the retry topic error handler as "not due yet": seek back to the record without committing it
		throw new KafkaBackoffException("Record published after the reprocessing run started", partition,
				this.dlt.container().getListenerId(), Long.MAX_VALUE);
	}

	private void partitionCompleted(DltRun run, TopicPartition partition) {
		run.partitionCompleted(partition);
		if (run.isCompleteFor(this.dlt.container().getAssignedPartitions())) {
			this.runCompleted.accept(run);
		}
	}

	private void process(DltRun run, ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {

		int attempts = DltHeaders.attempts(record.headers());
		if (!this.filter.shouldReprocess(run.context(), record)) {
			this.publisher.requeue(this.dlt.operations(), record, attempts, null);
			run.skipped();
			acknowledge(acknowledgment);
			return;
		}
		try {
			invokeDelegate(record, acknowledgment, consumer);
			run.succeeded();
		}
		catch (Exception ex) {
			if (SeekUtils.isBackoffException(ex)) {
				throw ex;
			}
			handleFailure(run, record, attempts + 1, rootFailure(ex));
			acknowledge(acknowledgment);
		}
	}

	private void handleFailure(DltRun run, ConsumerRecord<K, V> record, int attempts, Throwable failure) {
		DltTopicSettings settings = this.dlt.settings();
		if (settings.parkingLotTopic() != null && attempts >= settings.maxAttempts()) {
			this.publisher.park(this.dlt.operations(), record, settings.parkingLotTopic(), attempts, failure);
			run.parked();
			this.logger.error(failure, () -> "Reprocessing of " + describe(record) + " failed " + attempts
					+ " time(s), record moved to " + settings.parkingLotTopic());
		}
		else {
			this.publisher.requeue(this.dlt.operations(), record, attempts, failure);
			run.failed();
			this.logger.warn(failure, () -> "Reprocessing of " + describe(record) + " failed " + attempts
					+ " time(s), record put back in " + record.topic());
		}
	}

	private void invokeDelegate(ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
			org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) {

		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE -> this.delegate.onMessage(record, acknowledgment, consumer);
			case ACKNOWLEDGING -> this.delegate.onMessage(record, acknowledgment);
			case CONSUMER_AWARE -> this.delegate.onMessage(record, consumer);
			case SIMPLE -> this.delegate.onMessage(record);
		}
	}

	private static void acknowledge(Acknowledgment acknowledgment) {
		if (acknowledgment != null) {
			acknowledgment.acknowledge();
		}
	}

	private static Throwable rootFailure(Exception ex) {
		Throwable failure = ex;
		while ((failure instanceof TimestampedException || failure instanceof ListenerExecutionFailedException)
				&& failure.getCause() != null) {
			failure = failure.getCause();
		}
		return failure;
	}

	private static String describe(ConsumerRecord<?, ?> record) {
		return record.topic() + "-" + record.partition() + "@" + record.offset();
	}

}
