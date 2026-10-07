package io.github.idird.kafka.dlq;

/**
 * Additional condition a scheduled trigger must satisfy before a dead letter topic is reprocessed, for example a
 * feature flag, an approval stored in a database or the health of a downstream system. Every bean of this type must
 * agree. Manual starts are not subject to conditions.
 */
@FunctionalInterface
public interface DltReprocessingCondition {

	boolean shouldTrigger(DltReprocessingContext context);

}
