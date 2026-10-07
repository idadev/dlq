package io.github.idird.kafka.dlq;

import java.time.Instant;

/**
 * Published when a reprocessing run starts.
 *
 * @param context what is reprocessed.
 * @param trigger what started the run.
 * @param startedAt start of the run.
 */
public record DltReprocessingStartedEvent(DltReprocessingContext context, DltRunTrigger trigger, Instant startedAt) {
}
