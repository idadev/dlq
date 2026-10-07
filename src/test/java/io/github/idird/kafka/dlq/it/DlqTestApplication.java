package io.github.idird.kafka.dlq.it;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.idird.kafka.dlq.DltReprocessingCondition;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * A service consuming three topics, each with its own retry and dead letter topics.
 */
@SpringBootApplication
public class DlqTestApplication {

	@Bean
	KafkaAdmin.NewTopics consumedTopics() {
		return new KafkaAdmin.NewTopics(TopicBuilder.name("orders").build(), TopicBuilder.name("payments").build(),
				TopicBuilder.name("invoices").build());
	}

	@Bean
	DltReprocessingCondition invoicesApproval(Probe probe) {
		return context -> !context.name().equals("invoices") || probe.invoicesApproved.get();
	}

	@Component
	static class OrdersListener {

		private final Probe probe;

		OrdersListener(Probe probe) {
			this.probe = probe;
		}

		@RetryableTopic(attempts = "2")
		@KafkaListener(topics = "orders")
		public void consume(String payload) {
			this.probe.consume("orders", payload);
		}

		@DltHandler
		public void reprocess(String payload) {
			this.probe.reprocess("orders", payload);
		}

	}

	@Component
	static class PaymentsListener {

		private final Probe probe;

		PaymentsListener(Probe probe) {
			this.probe = probe;
		}

		@RetryableTopic(attempts = "2")
		@KafkaListener(topics = "payments")
		public void consume(String payload) {
			this.probe.consume("payments", payload);
		}

		@DltHandler
		public void reprocess(String payload) {
			this.probe.reprocess("payments", payload);
		}

	}

	@Component
	static class InvoicesListener {

		private final Probe probe;

		InvoicesListener(Probe probe) {
			this.probe = probe;
		}

		@RetryableTopic(attempts = "2")
		@KafkaListener(topics = "invoices")
		public void consume(String payload) {
			this.probe.consume("invoices", payload);
		}

		@DltHandler
		public void reprocess(String payload) {
			this.probe.reprocess("invoices", payload);
		}

	}

	/**
	 * Records what the listeners receive and decides which payloads they fail on.
	 */
	@Component
	static class Probe {

		final List<String> consumed = new CopyOnWriteArrayList<>();

		final List<String> reprocessed = new CopyOnWriteArrayList<>();

		final Set<String> failingReprocessing = ConcurrentHashMap.newKeySet();

		final AtomicBoolean invoicesApproved = new AtomicBoolean(true);

		void consume(String topic, String payload) {
			if (!payload.startsWith("ok")) {
				throw new IllegalStateException("Cannot consume " + payload);
			}
			this.consumed.add(topic + ":" + payload);
		}

		void reprocess(String topic, String payload) {
			this.reprocessed.add(topic + ":" + payload);
			if (this.failingReprocessing.contains(payload)) {
				throw new IllegalStateException("Cannot reprocess " + payload);
			}
		}

		long reprocessingCount(String topic, String payload) {
			return this.reprocessed.stream().filter((topic + ":" + payload)::equals).count();
		}

	}

}
