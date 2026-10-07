package io.github.idird.kafka.dlq;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.retrytopic.DestinationTopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfiguredRetryTopicNamesProviderFactoryTest {

	private final ConfiguredRetryTopicNamesProviderFactory factory = new ConfiguredRetryTopicNamesProviderFactory(
			List.of(settings("orders", "orders.retry", "orders.dlq"), settings("payments", null, "payments-dlt")));

	@Test
	void mainTopicKeepsItsName() {
		assertThat(name(Type.MAIN, "", "orders")).isEqualTo("orders");
	}

	@Test
	void configuredNamesReplaceTheSuffixes() {
		assertThat(name(Type.RETRY, "-retry", "orders")).isEqualTo("orders.retry");
		assertThat(name(Type.DLT, "-dlt", "orders")).isEqualTo("orders.dlq");
	}

	@Test
	void retryIndexIsKeptWhenThereAreSeveralRetryTopics() {
		assertThat(name(Type.RETRY, "-retry-0", "orders")).isEqualTo("orders.retry-0");
		assertThat(name(Type.RETRY, "-retry-2000", "orders")).isEqualTo("orders.retry-2000");
	}

	@Test
	void retryTopicFallsBackToTheSuffixWhenNotConfigured() {
		assertThat(name(Type.RETRY, "-retry-1", "payments")).isEqualTo("payments-retry-1");
	}

	@Test
	void dltNameOfAManagedTopicIgnoresTheAnnotationSuffix() {
		assertThat(name(Type.DLT, ".custom", "payments")).isEqualTo("payments-dlt");
	}

	@Test
	void unmanagedTopicsKeepTheSuffixNaming() {
		assertThat(name(Type.RETRY, "-retry", "invoices")).isEqualTo("invoices-retry");
		assertThat(name(Type.DLT, "-dlt", "invoices")).isEqualTo("invoices-dlt");
	}

	private String name(Type type, String suffix, String topic) {
		DestinationTopic.Properties properties = mock(DestinationTopic.Properties.class);
		when(properties.suffix()).thenReturn(suffix);
		when(properties.isMainEndpoint()).thenReturn(type == Type.MAIN);
		when(properties.isDltTopic()).thenReturn(type == Type.DLT);
		return this.factory.createRetryTopicNamesProvider(properties).getTopicName(topic);
	}

	private static DltTopicSettings settings(String topic, String retryTopic, String dltTopic) {
		return new DltTopicSettings(topic, topic, retryTopic, dltTopic, null, null, ZoneId.systemDefault(), 1,
				Duration.ofSeconds(30), null, 3);
	}

	private enum Type {

		MAIN, RETRY, DLT

	}

}
