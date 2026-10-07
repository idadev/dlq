package io.github.idird.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DltReprocessingListenerAdapterTest {

	private static final TopicPartition PARTITION_0 = new TopicPartition("orders.dlq", 0);

	private static final TopicPartition PARTITION_1 = new TopicPartition("orders.dlq", 1);

	@SuppressWarnings("unchecked")
	private final MessageListener<String, String> delegate = mock(MessageListener.class);

	private final MessageListenerContainer container = mock(MessageListenerContainer.class);

	private final KafkaOperations<?, ?> operations = mock(KafkaOperations.class);

	private final DltRecordPublisher publisher = mock(DltRecordPublisher.class);

	private final Acknowledgment acknowledgment = mock(Acknowledgment.class);

	private final List<DltRun> completedRuns = new ArrayList<>();

	private boolean accepted = true;

	@Test
	void outsideARunTheAdapterIsTransparent() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		ConsumerRecord<String, String> record = record(0, 5);
		doThrow(new IllegalStateException("boom")).when(this.delegate).onMessage(record);

		assertThatIllegalStateException().isThrownBy(() -> adapter(dlt).onMessage(record, this.acknowledgment, null));

		verifyNoInteractions(this.publisher);
	}

	@Test
	void recordIsHandedToTheHandler() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 5);

		adapter(dlt).onMessage(record, this.acknowledgment, null);

		verify(this.delegate).onMessage(record);
		assertThat(run.summary(DltRunEndReason.COMPLETED).succeeded()).isEqualTo(1);
		assertThat(this.completedRuns).isEmpty();
	}

	@Test
	void failedRecordIsPutBackWithItsAttemptAndTheHandlerExceptionIsNotPropagated() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 5);
		IllegalStateException failure = new IllegalStateException("boom");
		doThrow(new ListenerExecutionFailedException("listener failed", failure)).when(this.delegate).onMessage(record);

		adapter(dlt).onMessage(record, this.acknowledgment, null);

		verify(this.publisher).requeue(this.operations, record, 1, failure);
		verify(this.acknowledgment).acknowledge();
		assertThat(run.summary(DltRunEndReason.COMPLETED).failed()).isEqualTo(1);
	}

	@Test
	void recordThatExhaustedItsAttemptsIsParked() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 5);
		record.headers().add(DltHeaders.ATTEMPTS, "1".getBytes(StandardCharsets.UTF_8));
		IllegalStateException failure = new IllegalStateException("boom");
		doThrow(failure).when(this.delegate).onMessage(record);

		adapter(dlt).onMessage(record, this.acknowledgment, null);

		verify(this.publisher).park(this.operations, record, "orders.parking", 2, failure);
		verify(this.publisher, never()).requeue(any(), any(), anyInt(), any());
		assertThat(run.summary(DltRunEndReason.COMPLETED).parked()).isEqualTo(1);
	}

	@Test
	void withoutParkingLotTheRecordStaysInTheDeadLetterTopic() {
		ManagedDlt dlt = dlt(null, 2);
		run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 5);
		record.headers().add(DltHeaders.ATTEMPTS, "7".getBytes(StandardCharsets.UTF_8));
		IllegalStateException failure = new IllegalStateException("boom");
		doThrow(failure).when(this.delegate).onMessage(record);

		adapter(dlt).onMessage(record, this.acknowledgment, null);

		verify(this.publisher).requeue(this.operations, record, 8, failure);
	}

	@Test
	void recordIsNotLostWhenItCannotBePutBack() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 9);
		doThrow(new IllegalStateException("boom")).when(this.delegate).onMessage(record);
		doThrow(new DltPublishException("broker down", null)).when(this.publisher).requeue(any(), any(), anyInt(),
				any());

		assertThatExceptionOfType(DltPublishException.class)
				.isThrownBy(() -> adapter(dlt).onMessage(record, this.acknowledgment, null));

		verify(this.acknowledgment, never()).acknowledge();
		assertThat(run.isCompleteFor(List.of(PARTITION_0))).isFalse();
	}

	@Test
	void rejectedRecordIsPutBackUntouched() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		ConsumerRecord<String, String> record = record(0, 5);
		record.headers().add(DltHeaders.ATTEMPTS, "1".getBytes(StandardCharsets.UTF_8));
		this.accepted = false;

		adapter(dlt).onMessage(record, this.acknowledgment, null);

		verify(this.delegate, never()).onMessage(record);
		verify(this.publisher).requeue(this.operations, record, 1, null);
		verify(this.acknowledgment).acknowledge();
		assertThat(run.summary(DltRunEndReason.COMPLETED).skipped()).isEqualTo(1);
	}

	@Test
	void recordPublishedAfterTheRunStartedIsLeftForTheNextRun() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		run(dlt, 10, 10);
		when(this.container.getAssignedPartitions()).thenReturn(List.of(PARTITION_0, PARTITION_1));
		ConsumerRecord<String, String> record = record(0, 10);

		assertThatExceptionOfType(KafkaBackoffException.class)
				.isThrownBy(() -> adapter(dlt).onMessage(record, this.acknowledgment, null));

		verify(this.container).pausePartition(PARTITION_0);
		verifyNoInteractions(this.delegate, this.publisher);
		assertThat(this.completedRuns).isEmpty();
	}

	@Test
	void runCompletesOnceEveryAssignedPartitionReachedItsBoundary() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = run(dlt, 10, 10);
		when(this.container.getAssignedPartitions()).thenReturn(List.of(PARTITION_0, PARTITION_1));
		DltReprocessingListenerAdapter<String, String> adapter = adapter(dlt);

		adapter.onMessage(record(0, 9), this.acknowledgment, null);
		assertThat(this.completedRuns).isEmpty();

		adapter.onMessage(record(1, 9), this.acknowledgment, null);
		assertThat(this.completedRuns).containsExactly(run);
	}

	@Test
	void partitionWithoutPendingRecordIsCompleteFromTheStart() {
		ManagedDlt dlt = dlt("orders.parking", 2);
		DltRun run = new DltRun(new DltReprocessingContext("orders", "orders", "orders.dlq", 1), DltRunTrigger.MANUAL,
				new DltOffsets(Map.of(PARTITION_0, 9L, PARTITION_1, 4L), Map.of(PARTITION_0, 10L, PARTITION_1, 4L)),
				Clock.systemUTC());
		dlt.runStarted(run);
		when(this.container.getAssignedPartitions()).thenReturn(List.of(PARTITION_0, PARTITION_1));

		adapter(dlt).onMessage(record(0, 9), this.acknowledgment, null);

		assertThat(this.completedRuns).containsExactly(run);
	}

	private DltReprocessingListenerAdapter<String, String> adapter(ManagedDlt dlt) {
		return new DltReprocessingListenerAdapter<>(this.delegate, dlt, this.publisher,
				(context, record) -> this.accepted, this.completedRuns::add);
	}

	private ManagedDlt dlt(String parkingLotTopic, int maxAttempts) {
		DltTopicSettings settings = new DltTopicSettings("orders", "orders", null, "orders.dlq", parkingLotTopic, null,
				ZoneId.systemDefault(), 1, Duration.ofSeconds(30), null, maxAttempts);
		return new ManagedDlt(settings, this.container, this.operations);
	}

	private static DltRun run(ManagedDlt dlt, long endOfPartition0, long endOfPartition1) {
		DltRun run = new DltRun(new DltReprocessingContext("orders", "orders", "orders.dlq", 2), DltRunTrigger.MANUAL,
				new DltOffsets(Map.of(PARTITION_0, 0L, PARTITION_1, 0L),
						Map.of(PARTITION_0, endOfPartition0, PARTITION_1, endOfPartition1)),
				Clock.systemUTC());
		dlt.runStarted(run);
		return run;
	}

	private static ConsumerRecord<String, String> record(int partition, long offset) {
		return new ConsumerRecord<>("orders.dlq", partition, offset, "key", "value");
	}

}
