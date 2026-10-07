package io.github.idird.kafka.dlq;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * A dead letter topic, its listener container and its reprocessing state.
 */
final class ManagedDlt {

	private final DltTopicSettings settings;

	private final MessageListenerContainer container;

	private final KafkaOperations<?, ?> operations;

	private final AtomicReference<DltRun> currentRun = new AtomicReference<>();

	private volatile DltRunSummary lastRun;

	private volatile ScheduledFuture<?> timeout;

	ManagedDlt(DltTopicSettings settings, MessageListenerContainer container, KafkaOperations<?, ?> operations) {
		this.settings = settings;
		this.container = container;
		this.operations = operations;
	}

	DltTopicSettings settings() {
		return this.settings;
	}

	MessageListenerContainer container() {
		return this.container;
	}

	KafkaOperations<?, ?> operations() {
		return this.operations;
	}

	DltRun currentRun() {
		return this.currentRun.get();
	}

	void runStarted(DltRun run) {
		this.currentRun.set(run);
	}

	void runEnded(DltRun run, DltRunSummary summary) {
		if (summary != null) {
			this.lastRun = summary;
		}
		this.currentRun.compareAndSet(run, null);
	}

	DltRunSummary lastRun() {
		return this.lastRun;
	}

	void timeout(ScheduledFuture<?> timeout) {
		this.timeout = timeout;
	}

	void cancelTimeout() {
		ScheduledFuture<?> scheduled = this.timeout;
		if (scheduled != null) {
			scheduled.cancel(false);
			this.timeout = null;
		}
	}

}
