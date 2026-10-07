package io.github.idird.kafka.dlq;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Headers added to the records that are re-published during a reprocessing run. Values are UTF-8 strings.
 */
public final class DltHeaders {

	/**
	 * Number of times the dead letter handler failed on this record.
	 */
	public static final String ATTEMPTS = "x-dlt-reprocessing-attempts";

	/**
	 * Class name of the exception thrown by the last failed reprocessing attempt.
	 */
	public static final String EXCEPTION_FQCN = "x-dlt-reprocessing-exception-fqcn";

	/**
	 * Message of the exception thrown by the last failed reprocessing attempt.
	 */
	public static final String EXCEPTION_MESSAGE = "x-dlt-reprocessing-exception-message";

	/**
	 * Dead letter topic a parked record comes from.
	 */
	public static final String PARKED_FROM = "x-dlt-parked-from";

	private DltHeaders() {
	}

	/**
	 * Reads the number of failed reprocessing attempts of a record.
	 * @param headers the record headers.
	 * @return the attempts, 0 when the record was never reprocessed.
	 */
	public static int attempts(Headers headers) {
		Header header = headers.lastHeader(ATTEMPTS);
		if (header == null || header.value() == null) {
			return 0;
		}
		try {
			return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
		}
		catch (NumberFormatException ex) {
			return 0;
		}
	}

}
