package io.github.idird.kafka.dlq;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Configuration of the retry / dead letter workflow, one entry per consumed topic.
 */
@ConfigurationProperties(prefix = "kafka.dlq")
public class KafkaDlqProperties {

	private static final String CRON_DISABLED = "-";

	/**
	 * Whether the library manages the dead letter topics declared under "topics".
	 */
	private boolean enabled = true;

	/**
	 * Whether the configured parking lot topics are created at startup (broker default partitions and replicas).
	 */
	private boolean autoCreateParkingLotTopics = true;

	/**
	 * Maximum time to wait for a record to be re-published to the dead letter or parking lot topic.
	 */
	private Duration publishTimeout = Duration.ofSeconds(30);

	/**
	 * Maximum time to wait for the admin requests used to count pending messages.
	 */
	private Duration adminTimeout = Duration.ofSeconds(10);

	/**
	 * Reprocessing settings applied to every topic that does not override them.
	 */
	private final Reprocessing defaults = new Reprocessing();

	/**
	 * Managed topics, keyed by a logical name used in logs, events and the manager API.
	 */
	private final Map<String, Topic> topics = new LinkedHashMap<>();

	public boolean isEnabled() {
		return this.enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public boolean isAutoCreateParkingLotTopics() {
		return this.autoCreateParkingLotTopics;
	}

	public void setAutoCreateParkingLotTopics(boolean autoCreateParkingLotTopics) {
		this.autoCreateParkingLotTopics = autoCreateParkingLotTopics;
	}

	public Duration getPublishTimeout() {
		return this.publishTimeout;
	}

	public void setPublishTimeout(Duration publishTimeout) {
		this.publishTimeout = publishTimeout;
	}

	public Duration getAdminTimeout() {
		return this.adminTimeout;
	}

	public void setAdminTimeout(Duration adminTimeout) {
		this.adminTimeout = adminTimeout;
	}

	public Reprocessing getDefaults() {
		return this.defaults;
	}

	public Map<String, Topic> getTopics() {
		return this.topics;
	}

	/**
	 * Merges each topic with the defaults and validates the result.
	 * @return the effective settings, in declaration order.
	 */
	public List<DltTopicSettings> resolveTopics() {
		List<DltTopicSettings> resolved = new ArrayList<>();
		Set<String> usedTopics = new HashSet<>();
		this.topics.forEach((name, entry) -> {
			String topic = StringUtils.hasText(entry.getTopic()) ? entry.getTopic() : name;
			String dltTopic = StringUtils.hasText(entry.getDltTopic()) ? entry.getDltTopic() : topic + "-dlt";
			String retryTopic = StringUtils.hasText(entry.getRetryTopic()) ? entry.getRetryTopic() : null;
			String parkingLotTopic = StringUtils.hasText(entry.getParkingLotTopic()) ? entry.getParkingLotTopic() : null;
			for (String used : new String[] { topic, retryTopic, dltTopic, parkingLotTopic }) {
				Assert.state(used == null || usedTopics.add(used),
						() -> "kafka.dlq.topics." + name + ": topic '" + used + "' is declared more than once");
			}

			String cron = first(entry.getCron(), this.defaults.getCron(), null);
			if (!StringUtils.hasText(cron) || CRON_DISABLED.equals(cron.trim())) {
				cron = null;
			}
			String validatedCron = cron;
			Assert.state(cron == null || CronExpression.isValidExpression(cron),
					() -> "kafka.dlq.topics." + name + ".cron: invalid cron expression '" + validatedCron + "'");
			String zone = first(entry.getZone(), this.defaults.getZone(), null);
			long minPendingMessages = first(entry.getMinPendingMessages(), this.defaults.getMinPendingMessages(), 1L);
			Duration idleTimeout = first(entry.getIdleTimeout(), this.defaults.getIdleTimeout(), Duration.ofSeconds(30));
			Duration maxDuration = first(entry.getMaxDuration(), this.defaults.getMaxDuration(), null);
			int maxAttempts = first(entry.getMaxAttempts(), this.defaults.getMaxAttempts(), 3);
			Assert.state(minPendingMessages >= 1, () -> "kafka.dlq.topics." + name + ".min-pending-messages must be >= 1");
			Assert.state(maxAttempts >= 1, () -> "kafka.dlq.topics." + name + ".max-attempts must be >= 1");
			Assert.state(!idleTimeout.isNegative() && !idleTimeout.isZero(),
					() -> "kafka.dlq.topics." + name + ".idle-timeout must be positive");

			resolved.add(new DltTopicSettings(name, topic, retryTopic, dltTopic, parkingLotTopic, cron,
					StringUtils.hasText(zone) ? ZoneId.of(zone) : ZoneId.systemDefault(), minPendingMessages,
					idleTimeout, maxDuration, maxAttempts));
		});
		return List.copyOf(resolved);
	}

	private static <T> T first(T value, T defaultValue, T fallback) {
		if (value != null) {
			return value;
		}
		return defaultValue != null ? defaultValue : fallback;
	}

	/**
	 * When and how a dead letter topic is reprocessed.
	 */
	public static class Reprocessing {

		/**
		 * Spring cron expression that triggers the reprocessing. Empty or "-" means manual trigger only.
		 */
		private String cron;

		/**
		 * Time zone of the cron expression. Defaults to the system time zone.
		 */
		private String zone;

		/**
		 * Minimum number of messages waiting in the dead letter topic for a scheduled trigger to start. Defaults to 1.
		 */
		private Long minPendingMessages;

		/**
		 * Reprocessing stops when no message has been received for this long. Defaults to 30s.
		 */
		private Duration idleTimeout;

		/**
		 * Hard limit on the duration of one reprocessing run. No limit by default.
		 */
		private Duration maxDuration;

		/**
		 * Number of failed reprocessing attempts after which a message is sent to the parking lot topic. Defaults to
		 * 3. Ignored without a parking lot topic: the message then stays in the dead letter topic.
		 */
		private Integer maxAttempts;

		public String getCron() {
			return this.cron;
		}

		public void setCron(String cron) {
			this.cron = cron;
		}

		public String getZone() {
			return this.zone;
		}

		public void setZone(String zone) {
			this.zone = zone;
		}

		public Long getMinPendingMessages() {
			return this.minPendingMessages;
		}

		public void setMinPendingMessages(Long minPendingMessages) {
			this.minPendingMessages = minPendingMessages;
		}

		public Duration getIdleTimeout() {
			return this.idleTimeout;
		}

		public void setIdleTimeout(Duration idleTimeout) {
			this.idleTimeout = idleTimeout;
		}

		public Duration getMaxDuration() {
			return this.maxDuration;
		}

		public void setMaxDuration(Duration maxDuration) {
			this.maxDuration = maxDuration;
		}

		public Integer getMaxAttempts() {
			return this.maxAttempts;
		}

		public void setMaxAttempts(Integer maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

	}

	/**
	 * A consumed topic with its retry, dead letter and parking lot topics.
	 */
	public static class Topic extends Reprocessing {

		/**
		 * Name of the consumed topic, as declared on the @KafkaListener. Defaults to the entry key.
		 */
		private String topic;

		/**
		 * Name of the retry topic. With several retry topics, the index or delay is appended ("-0", "-1"...).
		 * Defaults to the suffix declared on @RetryableTopic.
		 */
		private String retryTopic;

		/**
		 * Name of the dead letter topic. Defaults to the topic name followed by "-dlt".
		 */
		private String dltTopic;

		/**
		 * Topic receiving the messages that exhausted their reprocessing attempts. They are never replayed
		 * automatically.
		 */
		private String parkingLotTopic;

		public String getTopic() {
			return this.topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getRetryTopic() {
			return this.retryTopic;
		}

		public void setRetryTopic(String retryTopic) {
			this.retryTopic = retryTopic;
		}

		public String getDltTopic() {
			return this.dltTopic;
		}

		public void setDltTopic(String dltTopic) {
			this.dltTopic = dltTopic;
		}

		public String getParkingLotTopic() {
			return this.parkingLotTopic;
		}

		public void setParkingLotTopic(String parkingLotTopic) {
			this.parkingLotTopic = parkingLotTopic;
		}

	}

}
