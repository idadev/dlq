package io.github.idird.kafka.dlq;

/**
 * State of a managed dead letter topic.
 *
 * @param settings the effective settings.
 * @param running whether a run is in progress.
 * @param lastRun the last finished run, {@code null} if none since startup.
 */
public record DltReprocessingStatus(DltTopicSettings settings, boolean running, DltRunSummary lastRun) {
}
