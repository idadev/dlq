package io.github.idird.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.core.KafkaOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DltRecordPublisherTest {

	private final DltRecordPublisher publisher = new DltRecordPublisher(Duration.ofSeconds(1));

	@SuppressWarnings("unchecked")
	private final KafkaOperations<Object, Object> operations = mock(KafkaOperations.class);

	private final ConsumerRecord<String, String> record = new ConsumerRecord<>("orders.dlq", 2, 42L, "key", "value");

	@BeforeEach
	void setUp() {
		this.record.headers().add("kafka_dlt-original-topic", bytes("orders"));
		when(this.operations.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
	}

	@Test
	void requeueKeepsTheRecordAndCountsTheFailedAttempt() {
		this.record.headers().add(DltHeaders.ATTEMPTS, bytes("1"));

		this.publisher.requeue(this.operations, this.record, 2, new IllegalStateException("boom"));

		ProducerRecord<Object, Object> sent = sent();
		assertThat(sent.topic()).isEqualTo("orders.dlq");
		assertThat(sent.partition()).isNull();
		assertThat(sent.key()).isEqualTo("key");
		assertThat(sent.value()).isEqualTo("value");
		assertThat(header(sent, "kafka_dlt-original-topic")).isEqualTo("orders");
		assertThat(sent.headers().headers(DltHeaders.ATTEMPTS)).hasSize(1);
		assertThat(DltHeaders.attempts(sent.headers())).isEqualTo(2);
		assertThat(header(sent, DltHeaders.EXCEPTION_FQCN)).isEqualTo(IllegalStateException.class.getName());
		assertThat(header(sent, DltHeaders.EXCEPTION_MESSAGE)).isEqualTo("boom");
	}

	@Test
	void requeueOfARecordThatWasNotAttemptedLeavesItUntouched() {
		this.publisher.requeue(this.operations, this.record, 0, null);

		ProducerRecord<Object, Object> sent = sent();
		assertThat(sent.headers().toArray()).extracting(Header::key).containsExactly("kafka_dlt-original-topic");
	}

	@Test
	void parkSendsToTheParkingLotTopicWithItsOrigin() {
		this.publisher.park(this.operations, this.record, "orders.parking", 3, new IllegalStateException("boom"));

		ProducerRecord<Object, Object> sent = sent();
		assertThat(sent.topic()).isEqualTo("orders.parking");
		assertThat(sent.value()).isEqualTo("value");
		assertThat(header(sent, DltHeaders.PARKED_FROM)).isEqualTo("orders.dlq");
		assertThat(DltHeaders.attempts(sent.headers())).isEqualTo(3);
	}

	@Test
	void sendFailureIsReported() {
		when(this.operations.send(any(ProducerRecord.class)))
				.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

		assertThatExceptionOfType(DltPublishException.class)
				.isThrownBy(() -> this.publisher.requeue(this.operations, this.record, 1, null))
				.withRootCauseInstanceOf(IllegalStateException.class);
	}

	@Test
	void sendThatIsNotAcknowledgedInTimeIsReported() {
		when(this.operations.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

		assertThatExceptionOfType(DltPublishException.class).isThrownBy(
				() -> new DltRecordPublisher(Duration.ofMillis(50)).requeue(this.operations, this.record, 1, null));
	}

	@SuppressWarnings("unchecked")
	private ProducerRecord<Object, Object> sent() {
		ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		verify(this.operations).send(captor.capture());
		return captor.getValue();
	}

	private static String header(ProducerRecord<?, ?> record, String name) {
		return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

}
