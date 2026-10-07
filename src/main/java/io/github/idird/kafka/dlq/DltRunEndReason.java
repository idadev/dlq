package io.github.idird.kafka.dlq;

/**
 * Why a reprocessing run ended.
 */
public enum DltRunEndReason {

	/**
	 * Every message present when the run started has been handled.
	 */
	COMPLETED,

	/**
	 * No message was received during the idle timeout.
	 */
	IDLE,

	/**
	 * The maximum duration was reached.
	 */
	TIMEOUT,

	/**
	 * The run was stopped through the manager.
	 */
	STOPPED

}
