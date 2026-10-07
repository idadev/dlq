package io.github.idird.kafka.dlq;

/**
 * What is about to be, or is being, reprocessed.
 *
 * @param name logical name of the managed topic.
 * @param topic the consumed topic.
 * @param dltTopic the dead letter topic.
 * @param pendingMessages number of messages waiting in the dead letter topic when the trigger was evaluated.
 */
public record DltReprocessingContext(String name, String topic, String dltTopic, long pendingMessages) {
}
