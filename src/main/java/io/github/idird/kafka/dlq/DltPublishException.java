package io.github.idird.kafka.dlq;

import org.springframework.kafka.KafkaException;

/**
 * A record could not be re-published during a reprocessing run.
 */
public class DltPublishException extends KafkaException {

	private static final long serialVersionUID = 1L;

	public DltPublishException(String message, Throwable cause) {
		super(message, cause);
	}

}
