package io.github.idird.kafka.dlq;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Effective settings of one managed topic, once merged with the defaults.
 *
 * @param name logical name of the entry.
 * @param topic the consumed topic.
 * @param retryTopic the configured retry topic name, or {@code null} to keep the Spring Kafka naming.
 * @param dltTopic the dead letter topic.
 * @param parkingLotTopic the parking lot topic, or {@code null} when exhausted messages stay in the dead letter topic.
 * @param cron the cron expression, or {@code null} when reprocessing is only triggered manually.
 * @param zone the time zone of the cron expression.
 * @param minPendingMessages minimum number of pending messages for a scheduled trigger.
 * @param idleTimeout inactivity after which a run stops.
 * @param maxDuration maximum duration of a run, or {@code null} for no limit.
 * @param maxAttempts failed reprocessing attempts before parking.
 */
public record DltTopicSettings(String name, String topic, String retryTopic, String dltTopic, String parkingLotTopic,
		String cron, ZoneId zone, long minPendingMessages, Duration idleTimeout, Duration maxDuration,
		int maxAttempts) {

	public boolean scheduled() {
		return this.cron != null;
	}

}
