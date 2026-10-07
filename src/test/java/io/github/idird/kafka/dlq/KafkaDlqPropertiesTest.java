package io.github.idird.kafka.dlq;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class KafkaDlqPropertiesTest {

	@Test
	void topicWithoutSettingsUsesBuiltInDefaults() {
		List<DltTopicSettings> topics = bind(Map.of("kafka.dlq.topics.orders.retry-topic", "")).resolveTopics();

		assertThat(topics).containsExactly(new DltTopicSettings("orders", "orders", null, "orders-dlt", null, null,
				ZoneId.systemDefault(), 1, Duration.ofSeconds(30), null, 3));
		assertThat(topics.get(0).scheduled()).isFalse();
	}

	@Test
	void topicInheritsDefaultsAndOverridesThem() {
		Map<String, String> source = new HashMap<>();
		source.put("kafka.dlq.defaults.cron", "0 0 2 * * *");
		source.put("kafka.dlq.defaults.zone", "Europe/Paris");
		source.put("kafka.dlq.defaults.idle-timeout", "5s");
		source.put("kafka.dlq.defaults.max-attempts", "5");
		source.put("kafka.dlq.topics.orders.topic", "orders.v1");
		source.put("kafka.dlq.topics.orders.retry-topic", "orders.v1.retry");
		source.put("kafka.dlq.topics.orders.dlt-topic", "orders.v1.dlq");
		source.put("kafka.dlq.topics.orders.parking-lot-topic", "orders.v1.parking");
		source.put("kafka.dlq.topics.orders.min-pending-messages", "10");
		source.put("kafka.dlq.topics.orders.max-duration", "15m");
		source.put("kafka.dlq.topics.payments.cron", "-");
		source.put("kafka.dlq.topics.payments.max-attempts", "1");

		List<DltTopicSettings> topics = bind(source).resolveTopics();

		assertThat(topics).containsExactly(
				new DltTopicSettings("orders", "orders.v1", "orders.v1.retry", "orders.v1.dlq", "orders.v1.parking",
						"0 0 2 * * *", ZoneId.of("Europe/Paris"), 10, Duration.ofSeconds(5), Duration.ofMinutes(15), 5),
				new DltTopicSettings("payments", "payments", null, "payments-dlt", null, null,
						ZoneId.of("Europe/Paris"), 1, Duration.ofSeconds(5), null, 1));
		assertThat(topics.get(0).scheduled()).isTrue();
	}

	@Test
	void invalidCronIsRejected() {
		KafkaDlqProperties properties = bind(Map.of("kafka.dlq.topics.orders.cron", "every night"));

		assertThatIllegalStateException().isThrownBy(properties::resolveTopics)
				.withMessageContaining("kafka.dlq.topics.orders.cron");
	}

	@Test
	void sameTopicCannotBeDeclaredTwice() {
		KafkaDlqProperties properties = bind(Map.of("kafka.dlq.topics.a.topic", "orders",
				"kafka.dlq.topics.a.dlt-topic", "shared.dlq", "kafka.dlq.topics.b.topic", "payments",
				"kafka.dlq.topics.b.dlt-topic", "shared.dlq"));

		assertThatIllegalStateException().isThrownBy(properties::resolveTopics).withMessageContaining("shared.dlq");
	}

	@Test
	void maxAttemptsMustBePositive() {
		KafkaDlqProperties properties = bind(Map.of("kafka.dlq.topics.orders.max-attempts", "0"));

		assertThatIllegalStateException().isThrownBy(properties::resolveTopics).withMessageContaining("max-attempts");
	}

	private static KafkaDlqProperties bind(Map<String, String> source) {
		return new Binder(new MapConfigurationPropertySource(source)).bind("kafka.dlq", KafkaDlqProperties.class).get();
	}

}
