package io.github.idird.kafka.dlq;

/**
 * Published when a reprocessing run ends.
 *
 * @param summary outcome of the run.
 */
public record DltReprocessingCompletedEvent(DltRunSummary summary) {
}
