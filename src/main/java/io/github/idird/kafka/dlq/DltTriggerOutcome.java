package io.github.idird.kafka.dlq;

/**
 * Result of a request to reprocess a dead letter topic.
 */
public enum DltTriggerOutcome {

	STARTED,

	ALREADY_RUNNING,

	NOTHING_TO_PROCESS,

	CONDITIONS_NOT_MET

}
