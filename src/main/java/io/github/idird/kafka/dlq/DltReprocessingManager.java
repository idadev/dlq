package io.github.idird.kafka.dlq;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.DestinationTopicContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.Assert;

/**
 * Controls the listener containers of the managed dead letter topics. The containers are kept stopped; a reprocessing
 * run starts one of them, on its cron schedule when the trigger conditions are met or on demand, and stops it once
 * the messages present at the start of the run have been handled.
 */
public class DltReprocessingManager implements SmartLifecycle, ApplicationListener<ListenerContainerIdleEvent> {

	private static final LogAccessor logger = new LogAccessor(DltReprocessingManager.class);

	private final List<DltTopicSettings> settings;

	private final Supplier<KafkaListenerEndpointRegistry> registry;

	private final Supplier<DestinationTopicContainer> destinationTopics;

	private final DltOffsetInspector offsetInspector;

	private final DltRecordPublisher publisher;

	private final List<DltReprocessingCondition> conditions;

	private final List<DltRecordFilter> filters;

	private final ApplicationEventPublisher eventPublisher;

	private final ThreadPoolTaskScheduler scheduler;

	private final Clock clock;

	private final Map<String, ManagedDlt> managed = new LinkedHashMap<>();

	private volatile boolean running;

	/**
	 * Creates the manager.
	 * @param settings the managed topics.
	 * @param registry the registry holding the listener containers, resolved when the manager starts: a bean
	 * dependency on the registry would make Spring start it, and the dead letter containers, before the manager.
	 * @param destinationTopics access to the retry topic chains, resolved when the manager starts.
	 * @param offsetInspector counts the pending messages.
	 * @param publisher re-publishes the records that are not handled.
	 * @param conditions the conditions of the scheduled triggers.
	 * @param filters the record filters.
	 * @param eventPublisher publishes the run events.
	 * @param scheduler runs the cron triggers and stops the containers; owned by the manager.
	 * @param clock the clock.
	 */
	public DltReprocessingManager(List<DltTopicSettings> settings, Supplier<KafkaListenerEndpointRegistry> registry,
			Supplier<DestinationTopicContainer> destinationTopics, DltOffsetInspector offsetInspector,
			DltRecordPublisher publisher, List<DltReprocessingCondition> conditions, List<DltRecordFilter> filters,
			ApplicationEventPublisher eventPublisher, ThreadPoolTaskScheduler scheduler, Clock clock) {

		this.settings = settings;
		this.registry = registry;
		this.destinationTopics = destinationTopics;
		this.offsetInspector = offsetInspector;
		this.publisher = publisher;
		this.conditions = conditions;
		this.filters = filters;
		this.eventPublisher = eventPublisher;
		this.scheduler = scheduler;
		this.clock = clock;
	}

	/**
	 * Names of the managed topics.
	 * @return the logical names.
	 */
	public Set<String> names() {
		return Set.copyOf(this.managed.keySet());
	}

	/**
	 * Starts a reprocessing run on demand, whatever the trigger conditions.
	 * @param name logical name of the managed topic.
	 * @return the outcome.
	 */
	public DltTriggerOutcome start(String name) {
		return start(get(name), DltRunTrigger.MANUAL);
	}

	/**
	 * Starts a reprocessing run if the trigger conditions are met. This is what the cron schedule invokes.
	 * @param name logical name of the managed topic.
	 * @return the outcome.
	 */
	public DltTriggerOutcome trigger(String name) {
		return start(get(name), DltRunTrigger.SCHEDULED);
	}

	/**
	 * Stops the run in progress, if any. Must not be called from a listener thread.
	 * @param name logical name of the managed topic.
	 * @return whether a run was stopped.
	 */
	public boolean stop(String name) {
		ManagedDlt dlt = get(name);
		DltRun run = dlt.currentRun();
		return run != null && finish(dlt, run, DltRunEndReason.STOPPED);
	}

	/**
	 * Counts the messages waiting in the dead letter topic.
	 * @param name logical name of the managed topic.
	 * @return the lag of the dead letter listener.
	 */
	public long pendingMessages(String name) {
		ManagedDlt dlt = get(name);
		return this.offsetInspector.inspect(dlt.container().getGroupId(), dlt.settings().dltTopic()).pending();
	}

	public DltReprocessingStatus status(String name) {
		ManagedDlt dlt = get(name);
		return new DltReprocessingStatus(dlt.settings(), dlt.currentRun() != null, dlt.lastRun());
	}

	public List<DltReprocessingStatus> statuses() {
		return this.managed.keySet().stream().map(this::status).toList();
	}

	private ManagedDlt get(String name) {
		ManagedDlt dlt = this.managed.get(name);
		Assert.notNull(dlt, () -> "No managed dead letter topic named '" + name + "', known names: "
				+ this.managed.keySet());
		return dlt;
	}

	private DltTriggerOutcome start(ManagedDlt dlt, DltRunTrigger trigger) {
		DltTopicSettings topic = dlt.settings();
		DltRun run;
		synchronized (dlt) {
			if (dlt.currentRun() != null || dlt.container().isRunning()) {
				return DltTriggerOutcome.ALREADY_RUNNING;
			}
			DltOffsets offsets = this.offsetInspector.inspect(dlt.container().getGroupId(), topic.dltTopic());
			DltReprocessingContext context = new DltReprocessingContext(topic.name(), topic.topic(), topic.dltTopic(),
					offsets.pending());
			if (context.pendingMessages() == 0) {
				return DltTriggerOutcome.NOTHING_TO_PROCESS;
			}
			if (trigger == DltRunTrigger.SCHEDULED && (context.pendingMessages() < topic.minPendingMessages()
					|| !this.conditions.stream().allMatch(condition -> condition.shouldTrigger(context)))) {
				return DltTriggerOutcome.CONDITIONS_NOT_MET;
			}
			run = new DltRun(context, trigger, offsets, this.clock);
			dlt.runStarted(run);
			try {
				dlt.container().start();
			}
			catch (RuntimeException ex) {
				dlt.runEnded(run, null);
				throw ex;
			}
			if (topic.maxDuration() != null) {
				dlt.timeout(this.scheduler.schedule(() -> finish(dlt, run, DltRunEndReason.TIMEOUT),
						this.clock.instant().plus(topic.maxDuration())));
			}
		}
		logger.info(() -> "Reprocessing of " + topic.dltTopic() + " started (" + trigger + "), "
				+ run.context().pendingMessages() + " message(s) pending");
		this.eventPublisher.publishEvent(new DltReprocessingStartedEvent(run.context(), trigger, run.startedAt()));
		return DltTriggerOutcome.STARTED;
	}

	private boolean finish(ManagedDlt dlt, DltRun run, DltRunEndReason reason) {
		if (!run.beginFinish()) {
			return false;
		}
		dlt.cancelTimeout();
		try {
			dlt.container().stop();
		}
		finally {
			DltRunSummary summary = run.summary(reason);
			dlt.runEnded(run, summary);
			logger.info(() -> "Reprocessing of " + dlt.settings().dltTopic() + " ended (" + reason + "): "
					+ summary.succeeded() + " succeeded, " + summary.failed() + " failed, " + summary.parked()
					+ " parked, " + summary.skipped() + " skipped");
			this.eventPublisher.publishEvent(new DltReprocessingCompletedEvent(summary));
		}
		return true;
	}

	/**
	 * Ends a run from a listener thread, which cannot stop its own container.
	 */
	private void finishAsync(ManagedDlt dlt, DltRun run, DltRunEndReason reason) {
		if (this.running) {
			this.scheduler.execute(() -> finish(dlt, run, reason));
		}
	}

	@Override
	public void onApplicationEvent(ListenerContainerIdleEvent event) {
		MessageListenerContainer source = event.getContainer(MessageListenerContainer.class);
		for (ManagedDlt dlt : this.managed.values()) {
			DltRun run = dlt.currentRun();
			if (dlt.container() == source && run != null && isIdle(dlt, run)) {
				finishAsync(dlt, run, DltRunEndReason.IDLE);
			}
		}
	}

	private boolean isIdle(ManagedDlt dlt, DltRun run) {
		// the event is per consumer: with a concurrency above 1, the other consumers may still be working
		Duration quiet = Duration.between(run.lastActivity(), this.clock.instant());
		Collection<TopicPartition> assigned = dlt.container().getAssignedPartitions();
		Duration idleTimeout = dlt.settings().idleTimeout();
		// without assignment the consumer may still be joining its group
		return quiet.compareTo(assigned == null || assigned.isEmpty() ? idleTimeout.multipliedBy(2) : idleTimeout) >= 0;
	}

	@Override
	public void start() {
		if (this.running) {
			return;
		}
		if (this.managed.isEmpty()) {
			this.settings.forEach(topic -> this.managed.put(topic.name(), prepare(topic)));
		}
		this.scheduler.initialize();
		this.running = true;
		this.managed.values().stream().map(ManagedDlt::settings).filter(DltTopicSettings::scheduled)
				.forEach(topic -> this.scheduler.schedule(() -> scheduledTrigger(topic),
						new CronTrigger(topic.cron(), topic.zone())));
	}

	private void scheduledTrigger(DltTopicSettings topic) {
		try {
			DltTriggerOutcome outcome = trigger(topic.name());
			logger.debug(() -> "Scheduled reprocessing of " + topic.dltTopic() + ": " + outcome);
		}
		catch (RuntimeException ex) {
			logger.error(ex, () -> "Scheduled reprocessing of " + topic.dltTopic() + " could not start");
		}
	}

	@SuppressWarnings("unchecked")
	private ManagedDlt prepare(DltTopicSettings topic) {
		MessageListenerContainer container = findContainer(topic);
		ManagedDlt dlt = new ManagedDlt(topic, container, operations(topic, container));

		ContainerProperties properties = container.getContainerProperties();
		Object listener = properties.getMessageListener();
		Assert.state(listener instanceof MessageListener, () -> "The dead letter listener of " + topic.dltTopic()
				+ " must be a record listener");
		DltRecordFilter filter = (context, record) -> this.filters.stream()
				.allMatch(candidate -> candidate.shouldReprocess(context, record));
		container.setupMessageListener(new DltReprocessingListenerAdapter<>((MessageListener<Object, Object>) listener,
				dlt, this.publisher, filter, run -> finishAsync(dlt, run, DltRunEndReason.COMPLETED)));

		container.setAutoStartup(false);
		if (container.isRunning()) {
			logger.warn(() -> "The dead letter listener of " + topic.dltTopic() + " was already started, stopping it");
			container.stop();
		}
		properties.setIdleEventInterval(topic.idleTimeout().toMillis());
		// the container first starts long after the dead letter topic received its first messages
		properties.getKafkaConsumerProperties().setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return dlt;
	}

	private MessageListenerContainer findContainer(DltTopicSettings topic) {
		List<MessageListenerContainer> containers = this.registry.get().getListenerContainers().stream()
				.filter(candidate -> {
					String[] topics = candidate.getContainerProperties().getTopics();
					return topics != null && Arrays.asList(topics).contains(topic.dltTopic());
				}).toList();
		Assert.state(!containers.isEmpty(), () -> "kafka.dlq.topics." + topic.name() + ": no listener consumes the "
				+ "dead letter topic '" + topic.dltTopic() + "'. Check that a @RetryableTopic listener consumes '"
				+ topic.topic() + "' and that 'dlt-topic' matches the name of its dead letter topic.");
		Assert.state(containers.size() == 1, () -> "kafka.dlq.topics." + topic.name() + ": several listeners consume "
				+ "the dead letter topic '" + topic.dltTopic() + "'");
		MessageListenerContainer container = containers.get(0);
		Assert.state(container.getContainerProperties().getTopics().length == 1, () -> "kafka.dlq.topics."
				+ topic.name() + ": the dead letter listener of '" + topic.dltTopic() + "' also consumes other "
				+ "topics. Declare one @KafkaListener per managed topic.");
		return container;
	}

	private KafkaOperations<?, ?> operations(DltTopicSettings topic, MessageListenerContainer container) {
		String mainListenerId = container.getMainListenerId();
		Assert.state(mainListenerId != null, () -> "kafka.dlq.topics." + topic.name() + ": '" + topic.dltTopic()
				+ "' is not consumed by the dead letter listener of a @RetryableTopic");
		DestinationTopic destination = this.destinationTopics.get().getDestinationTopicByName(mainListenerId,
				topic.dltTopic());
		Assert.state(destination.isDltTopic(), () -> "kafka.dlq.topics." + topic.name() + ": '" + topic.dltTopic()
				+ "' is not the dead letter topic of a @RetryableTopic");
		return destination.getKafkaOperations();
	}

	@Override
	public void stop() {
		this.running = false;
		// the registry has a later phase: the containers are already stopped
		this.managed.values().forEach(ManagedDlt::cancelTimeout);
		this.scheduler.shutdown();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Starts before the listener registry so that the dead letter containers are not auto started.
	 */
	@Override
	public int getPhase() {
		return AbstractMessageListenerContainer.DEFAULT_PHASE - 100;
	}

}
