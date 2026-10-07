package io.github.idird.kafka.dlq;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.SuffixingRetryTopicNamesProviderFactory.SuffixingRetryTopicNamesProvider;

/**
 * Names the retry and dead letter topics of the managed topics from the configuration. Topics that are not managed, as
 * well as listener ids and group ids, keep the Spring Kafka suffix based naming.
 */
public class ConfiguredRetryTopicNamesProviderFactory implements RetryTopicNamesProviderFactory {

	// Spring Kafka appends the retry index or delay to the suffix when a listener has several retry topics
	private static final Pattern RETRY_INDEX = Pattern.compile("-\\d+$");

	private final Map<String, DltTopicSettings> settingsByTopic;

	public ConfiguredRetryTopicNamesProviderFactory(Collection<DltTopicSettings> settings) {
		this.settingsByTopic = settings.stream().collect(Collectors.toMap(DltTopicSettings::topic, Function.identity()));
	}

	@Override
	public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties properties) {
		return new SuffixingRetryTopicNamesProvider(properties) {

			@Override
			public String getTopicName(String topic) {
				DltTopicSettings settings = ConfiguredRetryTopicNamesProviderFactory.this.settingsByTopic.get(topic);
				if (settings == null || properties.isMainEndpoint()) {
					return super.getTopicName(topic);
				}
				if (properties.isDltTopic()) {
					return settings.dltTopic();
				}
				if (settings.retryTopic() == null) {
					return super.getTopicName(topic);
				}
				Matcher index = RETRY_INDEX.matcher(properties.suffix());
				return index.find() ? settings.retryTopic() + index.group() : settings.retryTopic();
			}

		};
	}

}
