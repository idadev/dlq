package io.github.idird.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.kafka.core.KafkaOperations;

/**
 * Re-publishes dead letter records, with their key, value and headers, and waits for the broker acknowledgment.
 */
public class DltRecordPublisher {

	private static final int MAX_MESSAGE_LENGTH = 1000;

	private final Duration timeout;

	public DltRecordPublisher(Duration timeout) {
		this.timeout = timeout;
	}

	/**
	 * Puts the record back at the end of its dead letter topic.
	 * @param operations the template the dead letter topic is published with.
	 * @param record the record.
	 * @param attempts the failed reprocessing attempts so far.
	 * @param failure the exception of the attempt that just failed, {@code null} if the record was not attempted.
	 */
	public void requeue(KafkaOperations<?, ?> operations, ConsumerRecord<?, ?> record, int attempts, Throwable failure) {
		send(operations, record.topic(), record, headers(record, attempts, failure));
	}

	/**
	 * Moves the record to the parking lot topic.
	 * @param operations the template the dead letter topic is published with.
	 * @param record the record.
	 * @param parkingLotTopic the parking lot topic.
	 * @param attempts the failed reprocessing attempts.
	 * @param failure the exception of the last attempt.
	 */
	public void park(KafkaOperations<?, ?> operations, ConsumerRecord<?, ?> record, String parkingLotTopic,
			int attempts, Throwable failure) {

		Headers headers = headers(record, attempts, failure);
		set(headers, DltHeaders.PARKED_FROM, record.topic());
		send(operations, parkingLotTopic, record, headers);
	}

	private Headers headers(ConsumerRecord<?, ?> record, int attempts, Throwable failure) {
		Headers headers = new RecordHeaders(record.headers().toArray());
		if (attempts > 0) {
			set(headers, DltHeaders.ATTEMPTS, Integer.toString(attempts));
		}
		if (failure != null) {
			set(headers, DltHeaders.EXCEPTION_FQCN, failure.getClass().getName());
			String message = String.valueOf(failure.getMessage());
			set(headers, DltHeaders.EXCEPTION_MESSAGE,
					message.length() > MAX_MESSAGE_LENGTH ? message.substring(0, MAX_MESSAGE_LENGTH) : message);
		}
		return headers;
	}

	private static void set(Headers headers, String name, String value) {
		headers.remove(name);
		headers.add(name, value.getBytes(StandardCharsets.UTF_8));
	}

	@SuppressWarnings("unchecked")
	private void send(KafkaOperations<?, ?> operations, String topic, ConsumerRecord<?, ?> record, Headers headers) {
		KafkaOperations<Object, Object> template = (KafkaOperations<Object, Object>) operations;
		ProducerRecord<Object, Object> outgoing = new ProducerRecord<>(topic, null, record.key(), record.value(),
				headers);
		try {
			CompletableFuture<?> result = (template.isTransactional() && !template.inTransaction()
					&& !template.isAllowNonTransactional())
							? template.executeInTransaction(inTransaction -> inTransaction.send(outgoing))
							: template.send(outgoing);
			result.get(this.timeout.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new DltPublishException("Interrupted while publishing to " + topic, ex);
		}
		catch (ExecutionException ex) {
			throw new DltPublishException("Failed to publish to " + topic, ex.getCause());
		}
		catch (TimeoutException | RuntimeException ex) {
			throw new DltPublishException("Failed to publish to " + topic, ex);
		}
	}

}
