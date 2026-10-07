package io.github.idird.kafka.dlq;

import java.time.Instant;

/**
 * Outcome of a reprocessing run.
 *
 * @param context what was reprocessed.
 * @param trigger what started the run.
 * @param startedAt start of the run.
 * @param endedAt end of the run.
 * @param endReason why the run ended.
 * @param succeeded records handled by the {@code @DltHandler}.
 * @param failed records the handler failed on, put back in the dead letter topic.
 * @param parked records the handler failed on, moved to the parking lot topic.
 * @param skipped records rejected by a {@link DltRecordFilter}, put back in the dead letter topic.
 */
public record DltRunSummary(DltReprocessingContext context, DltRunTrigger trigger, Instant startedAt, Instant endedAt,
		DltRunEndReason endReason, long succeeded, long failed, long parked, long skipped) {
}
