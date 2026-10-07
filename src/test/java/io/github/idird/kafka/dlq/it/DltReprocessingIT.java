package io.github.idird.kafka.dlq.it;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.github.idird.kafka.dlq.DltHeaders;
import io.github.idird.kafka.dlq.DltReprocessingManager;
import io.github.idird.kafka.dlq.DltRunEndReason;
import io.github.idird.kafka.dlq.DltRunSummary;
import io.github.idird.kafka.dlq.DltRunTrigger;
import io.github.idird.kafka.dlq.DltTriggerOutcome;
import io.github.idird.kafka.dlq.it.DlqTestApplication.Probe;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = DlqTestApplication.class)
class DltReprocessingIT {

	private static final Duration TIMEOUT = Duration.ofSeconds(60);

	private static final KafkaContainer KAFKA = new KafkaContainer(
			DockerImageName.parse(System.getProperty("kafka.image", "apache/kafka:4.3.1")));

	static {
		KAFKA.start();
	}

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	private KafkaAdmin kafkaAdmin;

	@Autowired
	private DltReprocessingManager manager;

	@Autowired
	private Probe probe;

	@DynamicPropertySource
	static void kafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
	}

	@Test
	void deadLettersWaitInTheConfiguredTopicUntilReprocessingIsRequested() throws Exception {
		this.template.send("orders", "ok-order");
		this.template.send("orders", "order-1");

		awaitPending("orders", 1);
		assertThat(this.probe.consumed).contains("orders:ok-order");
		assertThat(topics()).contains("orders.retry", "orders.dlq", "orders.parking", "payments.dlq", "invoices-dlt")
				.doesNotContain("orders-retry", "orders-dlt", "payments-dlt");
		await().during(Duration.ofSeconds(3)).atMost(TIMEOUT)
				.until(() -> this.probe.reprocessingCount("orders", "order-1") == 0);
		assertThat(this.manager.status("orders").running()).isFalse();

		DltRunSummary run = reprocess("orders");

		assertThat(this.probe.reprocessingCount("orders", "order-1")).isEqualTo(1);
		assertThat(run.trigger()).isEqualTo(DltRunTrigger.MANUAL);
		assertThat(run.endReason()).isEqualTo(DltRunEndReason.COMPLETED);
		assertThat(run.context().pendingMessages()).isEqualTo(1);
		assertThat(run.succeeded()).isEqualTo(1);
		assertThat(this.manager.pendingMessages("orders")).isZero();
		assertThat(this.manager.start("orders")).isEqualTo(DltTriggerOutcome.NOTHING_TO_PROCESS);
	}

	@Test
	void recordThatKeepsFailingIsRetriedOncePerRunThenParked() {
		this.probe.failingReprocessing.add("order-poison");
		this.template.send("orders", "order-poison");
		awaitPending("orders", 1);

		DltRunSummary firstRun = reprocess("orders");

		assertThat(firstRun.endReason()).isEqualTo(DltRunEndReason.COMPLETED);
		assertThat(firstRun.failed()).isEqualTo(1);
		assertThat(firstRun.parked()).isZero();
		assertThat(this.probe.reprocessingCount("orders", "order-poison")).isEqualTo(1);
		assertThat(this.manager.pendingMessages("orders")).isEqualTo(1);

		DltRunSummary secondRun = reprocess("orders");

		assertThat(secondRun.failed()).isZero();
		assertThat(secondRun.parked()).isEqualTo(1);
		assertThat(this.probe.reprocessingCount("orders", "order-poison")).isEqualTo(2);
		assertThat(this.manager.pendingMessages("orders")).isZero();

		List<ConsumerRecord<String, String>> parked = read("orders.parking", "order-poison");
		assertThat(parked).hasSize(1);
		assertThat(header(parked.get(0), DltHeaders.ATTEMPTS)).isEqualTo("2");
		assertThat(header(parked.get(0), DltHeaders.PARKED_FROM)).isEqualTo("orders.dlq");
		assertThat(header(parked.get(0), DltHeaders.EXCEPTION_FQCN)).isEqualTo(IllegalStateException.class.getName());
		assertThat(header(parked.get(0), DltHeaders.EXCEPTION_MESSAGE)).isEqualTo("Cannot reprocess order-poison");
		// the headers set by Spring Kafka on the way to the dead letter topic are kept
		assertThat(parked.get(0).headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS)).isNotNull();
	}

	@Test
	void topicsAreReprocessedIndependentlyAndWithoutParkingLotARecordStaysInItsDeadLetterTopic() {
		this.probe.failingReprocessing.add("payment-1");
		this.template.send("payments", "payment-1");
		awaitPending("payments", 1);

		for (int run = 1; run <= 3; run++) {
			assertThat(reprocess("payments").failed()).isEqualTo(1);
			assertThat(this.probe.reprocessingCount("payments", "payment-1")).isEqualTo(run);
			assertThat(this.manager.pendingMessages("payments")).isEqualTo(1);
		}
		assertThat(this.manager.status("orders").lastRun()).satisfiesAnyOf(lastRun -> assertThat(lastRun).isNull(),
				lastRun -> assertThat(lastRun.context().dltTopic()).isEqualTo("orders.dlq"));

		this.probe.failingReprocessing.remove("payment-1");
		DltRunSummary run = reprocess("payments");

		assertThat(run.succeeded()).isEqualTo(1);
		assertThat(this.probe.reprocessingCount("payments", "payment-1")).isEqualTo(4);
		assertThat(this.manager.pendingMessages("payments")).isZero();
		assertThat(this.probe.reprocessed).noneMatch(entry -> entry.startsWith("orders:payment"));
	}

	@Test
	void cronStartsTheReprocessingOnlyWhenTheTriggerConditionsAreMet() {
		this.template.send("invoices", "invoice-1");
		awaitPending("invoices", 1);

		// below min-pending-messages
		await().during(Duration.ofSeconds(5)).atMost(TIMEOUT).until(() -> this.probe.reprocessed.stream()
				.noneMatch(entry -> entry.startsWith("invoices:")));

		this.probe.invoicesApproved.set(false);
		this.template.send("invoices", "invoice-2");
		awaitPending("invoices", 2);

		// vetoed by the DltReprocessingCondition bean
		await().during(Duration.ofSeconds(5)).atMost(TIMEOUT).until(() -> this.probe.reprocessed.stream()
				.noneMatch(entry -> entry.startsWith("invoices:")));

		this.probe.invoicesApproved.set(true);

		await().atMost(TIMEOUT).until(() -> this.probe.reprocessed.contains("invoices:invoice-1")
				&& this.probe.reprocessed.contains("invoices:invoice-2"));
		await().atMost(TIMEOUT).until(() -> this.manager.status("invoices").lastRun() != null);
		DltRunSummary run = this.manager.status("invoices").lastRun();
		assertThat(run.trigger()).isEqualTo(DltRunTrigger.SCHEDULED);
		assertThat(run.succeeded()).isEqualTo(2);
		assertThat(this.manager.pendingMessages("invoices")).isZero();
	}

	private DltRunSummary reprocess(String name) {
		assertThat(this.manager.start(name)).isEqualTo(DltTriggerOutcome.STARTED);
		await().atMost(TIMEOUT).until(() -> !this.manager.status(name).running());
		return this.manager.status(name).lastRun();
	}

	private void awaitPending(String name, long expected) {
		await().atMost(TIMEOUT).until(() -> this.manager.pendingMessages(name) == expected);
	}

	private Set<String> topics() throws Exception {
		try (Admin admin = Admin.create(this.kafkaAdmin.getConfigurationProperties())) {
			return admin.listTopics().names().get();
		}
	}

	private List<ConsumerRecord<String, String>> read(String topic, String value) {
		Map<String, Object> config = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
				ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				"earliest", ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		List<ConsumerRecord<String, String>> records = new ArrayList<>();
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
			consumer.subscribe(List.of(topic));
			await().atMost(TIMEOUT).until(() -> {
				consumer.poll(Duration.ofMillis(500)).forEach(record -> {
					if (value.equals(record.value())) {
						records.add(record);
					}
				});
				return !records.isEmpty();
			});
			// anything else that would have been published by mistake
			consumer.poll(Duration.ofSeconds(2)).forEach(record -> {
				if (value.equals(record.value())) {
					records.add(record);
				}
			});
		}
		return records;
	}

	private static String header(ConsumerRecord<?, ?> record, String name) {
		return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
	}

}
