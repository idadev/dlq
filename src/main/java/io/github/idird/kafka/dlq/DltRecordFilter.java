package io.github.idird.kafka.dlq;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Selects the dead letter records a run hands to the {@code @DltHandler}. Rejected records are put back at the end of
 * the dead letter topic, untouched, and wait for a later run. Every bean of this type must accept the record.
 */
@FunctionalInterface
public interface DltRecordFilter {

	boolean shouldReprocess(DltReprocessingContext context, ConsumerRecord<?, ?> record);

}
