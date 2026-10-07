package io.github.idird.kafka.dlq;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.DestinationTopicContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DltReprocessingManagerTest {

	private static final TopicPartition PARTITION = new TopicPartition("orders.dlq", 0);

	private final KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);

	private final MessageListenerContainer container = mock(MessageListenerContainer.class);

	private final ContainerProperties containerProperties = new ContainerProperties("orders.dlq");

	private final DestinationTopicContainer destinationTopics = mock(DestinationTopicContainer.class);

	private final DestinationTopic destination = mock(DestinationTopic.class);

	private final DltOffsetInspector offsetInspector = mock(DltOffsetInspector.class);

	private final List<Object> events = new CopyOnWriteArrayList<>();

	private final AtomicBoolean conditionMet = new AtomicBoolean(true);

	private final TestClock clock = new TestClock();

	private DltReprocessingManager manager;

	@BeforeEach
	void setUp() {
		this.containerProperties.setMessageListener(mock(MessageListener.class));
		when(this.container.getContainerProperties()).thenReturn(this.containerProperties);
		when(this.container.getMainListenerId()).thenReturn("orders");
		when(this.container.getGroupId()).thenReturn("app-dlt");
		when(this.container.getAssignedPartitions()).thenReturn(List.of(PARTITION));
		when(this.registry.getListenerContainers()).thenReturn(List.of(this.container));
		when(this.destinationTopics.getDestinationTopicByName("orders", "orders.dlq")).thenReturn(this.destination);
		when(this.destination.isDltTopic()).thenReturn(true);
		doReturn(mock(KafkaOperations.class)).when(this.destination).getKafkaOperations();
		pending(3);
	}

	@AfterEach
	void tearDown() {
		if (this.manager != null) {
			this.manager.stop();
		}
	}

	@Test
	void deadLetterContainerIsTakenOverAtStartup() {
		start(settings(null, 1));

		verify(this.container).setAutoStartup(false);
		verify(this.container).setupMessageListener(any(DltReprocessingListenerAdapter.class));
		verify(this.container, never()).start();
		assertThat(this.containerProperties.getIdleEventInterval()).isEqualTo(5000L);
		assertThat(this.containerProperties.getKafkaConsumerProperties()).containsEntry("auto.offset.reset", "earliest");
		assertThat(this.manager.names()).containsExactly("orders");
		assertThat(this.manager.status("orders").running()).isFalse();
	}

	@Test
	void startupFailsWhenNoListenerConsumesTheDeadLetterTopic() {
		when(this.registry.getListenerContainers()).thenReturn(List.of());

		assertThatIllegalStateException().isThrownBy(() -> start(settings(null, 1)))
				.withMessageContaining("kafka.dlq.topics.orders").withMessageContaining("orders.dlq");
	}

	@Test
	void startupFailsWhenTheTopicIsNotADeadLetterTopic() {
		when(this.destination.isDltTopic()).thenReturn(false);

		assertThatIllegalStateException().isThrownBy(() -> start(settings(null, 1)))
				.withMessageContaining("not the dead letter topic");
	}

	@Test
	void manualStartRunsTheContainerAndPublishesAnEvent() {
		start(settings(null, 10));

		assertThat(this.manager.start("orders")).isEqualTo(DltTriggerOutcome.STARTED);

		verify(this.container).start();
		assertThat(this.manager.status("orders").running()).isTrue();
		assertThat(this.events).containsExactly(new DltReprocessingStartedEvent(
				new DltReprocessingContext("orders", "orders", "orders.dlq", 3), DltRunTrigger.MANUAL,
				this.clock.instant()));
		assertThat(this.manager.start("orders")).isEqualTo(DltTriggerOutcome.ALREADY_RUNNING);
	}

	@Test
	void nothingStartsWhenTheDeadLetterTopicIsEmpty() {
		start(settings(null, 1));
		pending(0);

		assertThat(this.manager.start("orders")).isEqualTo(DltTriggerOutcome.NOTHING_TO_PROCESS);

		verify(this.container, never()).start();
	}

	@Test
	void scheduledTriggerRequiresEnoughPendingMessages() {
		start(settings(null, 4));

		assertThat(this.manager.trigger("orders")).isEqualTo(DltTriggerOutcome.CONDITIONS_NOT_MET);

		pending(4);
		assertThat(this.manager.trigger("orders")).isEqualTo(DltTriggerOutcome.STARTED);
	}

	@Test
	void scheduledTriggerRequiresEveryCondition() {
		start(settings(null, 1));
		this.conditionMet.set(false);

		assertThat(this.manager.trigger("orders")).isEqualTo(DltTriggerOutcome.CONDITIONS_NOT_MET);
		assertThat(this.manager.start("orders")).isEqualTo(DltTriggerOutcome.STARTED);
	}

	@Test
	void cronTriggersTheReprocessing() {
		start(settings("* * * * * *", 1));

		verify(this.container, timeout(5000)).start();
		await().until(() -> !this.events.isEmpty());
		assertThat(this.events).first().isInstanceOfSatisfying(DltReprocessingStartedEvent.class,
				event -> assertThat(event.trigger()).isEqualTo(DltRunTrigger.SCHEDULED));
	}

	@Test
	void runEndsWhenTheContainerHasBeenIdleForTheTimeout() {
		start(settings(null, 1));
		this.manager.start("orders");

		this.clock.advance(Duration.ofSeconds(4));
		this.manager.onApplicationEvent(idleEvent());
		verify(this.container, never()).stop();

		this.clock.advance(Duration.ofSeconds(1));
		this.manager.onApplicationEvent(idleEvent());
		verify(this.container, timeout(5000)).stop();

		DltReprocessingStatus status = awaitEnded();
		assertThat(status.lastRun().endReason()).isEqualTo(DltRunEndReason.IDLE);
		assertThat(status.lastRun().trigger()).isEqualTo(DltRunTrigger.MANUAL);
		assertThat(this.events).last().isEqualTo(new DltReprocessingCompletedEvent(status.lastRun()));
	}

	@Test
	void consumerWithoutAssignmentGetsMoreTimeBeforeBeingConsideredIdle() {
		when(this.container.getAssignedPartitions()).thenReturn(List.of());
		start(settings(null, 1));
		this.manager.start("orders");

		this.clock.advance(Duration.ofSeconds(9));
		this.manager.onApplicationEvent(idleEvent());
		assertThat(this.manager.status("orders").running()).isTrue();
		verify(this.container, never()).stop();

		this.clock.advance(Duration.ofSeconds(1));
		this.manager.onApplicationEvent(idleEvent());
		verify(this.container, timeout(5000)).stop();
	}

	@Test
	void runCanBeStoppedOnDemand() {
		start(settings(null, 1));
		assertThat(this.manager.stop("orders")).isFalse();
		this.manager.start("orders");

		assertThat(this.manager.stop("orders")).isTrue();

		verify(this.container).stop();
		assertThat(this.manager.status("orders").running()).isFalse();
		assertThat(this.manager.status("orders").lastRun().endReason()).isEqualTo(DltRunEndReason.STOPPED);
	}

	@Test
	void runIsLimitedByItsMaximumDuration() {
		DltTopicSettings settings = new DltTopicSettings("orders", "orders", null, "orders.dlq", null, null,
				ZoneId.systemDefault(), 1, Duration.ofSeconds(5), Duration.ofMillis(200), 3);
		this.clock.useSystemTime();
		start(settings);

		this.manager.start("orders");

		verify(this.container, timeout(5000)).stop();
		assertThat(awaitEnded().lastRun().endReason()).isEqualTo(DltRunEndReason.TIMEOUT);
	}

	@Test
	void pendingMessagesAreCountedForTheGroupOfTheDeadLetterListener() {
		start(settings(null, 1));

		assertThat(this.manager.pendingMessages("orders")).isEqualTo(3);
		verify(this.offsetInspector).inspect("app-dlt", "orders.dlq");
	}

	private void start(DltTopicSettings settings) {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setDaemon(true);
		this.manager = new DltReprocessingManager(List.of(settings), () -> this.registry, () -> this.destinationTopics,
				this.offsetInspector, mock(DltRecordPublisher.class), List.of(context -> this.conditionMet.get()),
				List.of(), this.events::add, scheduler, this.clock);
		this.manager.start();
	}

	private DltReprocessingStatus awaitEnded() {
		await().until(() -> !this.manager.status("orders").running());
		return this.manager.status("orders");
	}

	private void pending(long count) {
		when(this.offsetInspector.inspect("app-dlt", "orders.dlq"))
				.thenReturn(new DltOffsets(Map.of(PARTITION, 0L), Map.of(PARTITION, count)));
	}

	private ListenerContainerIdleEvent idleEvent() {
		ListenerContainerIdleEvent event = mock(ListenerContainerIdleEvent.class);
		when(event.getContainer(MessageListenerContainer.class)).thenReturn(this.container);
		return event;
	}

	private static DltTopicSettings settings(String cron, long minPendingMessages) {
		return new DltTopicSettings("orders", "orders", null, "orders.dlq", null, cron, ZoneId.systemDefault(),
				minPendingMessages, Duration.ofSeconds(5), null, 3);
	}

	private static final class TestClock extends Clock {

		private volatile Instant now = Instant.parse("2026-01-01T00:00:00Z");

		private volatile boolean system;

		void advance(Duration duration) {
			this.now = this.now.plus(duration);
		}

		void useSystemTime() {
			this.system = true;
		}

		@Override
		public Instant instant() {
			return this.system ? Instant.now() : this.now;
		}

		@Override
		public ZoneId getZone() {
			return ZoneId.of("UTC");
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

	}

}
