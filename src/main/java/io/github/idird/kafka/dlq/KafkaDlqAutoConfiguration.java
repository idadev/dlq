package io.github.idird.kafka.dlq;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.retrytopic.DestinationTopicContainer;
import org.springframework.kafka.retrytopic.RetryTopicBeanNames;
import org.springframework.kafka.retrytopic.RetryTopicComponentFactory;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Auto-configuration of the dead letter reprocessing workflow, for Spring Boot 3.x and 4.x.
 */
@AutoConfiguration(afterName = { "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
		"org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration" })
@ConditionalOnClass(RetryableTopic.class)
@ConditionalOnBean(KafkaAdmin.class)
@ConditionalOnProperty(prefix = "kafka.dlq", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(KafkaDlqProperties.class)
public class KafkaDlqAutoConfiguration {

	/**
	 * Picked up by Spring Kafka to name the retry and dead letter topics from the configuration.
	 */
	@Bean
	@ConditionalOnMissingBean
	RetryTopicComponentFactory kafkaDlqRetryTopicComponentFactory(KafkaDlqProperties properties) {
		List<DltTopicSettings> settings = properties.resolveTopics();
		return new RetryTopicComponentFactory() {

			@Override
			public RetryTopicNamesProviderFactory retryTopicNamesProviderFactory() {
				return new ConfiguredRetryTopicNamesProviderFactory(settings);
			}

		};
	}

	@Bean
	@ConditionalOnMissingBean
	DltOffsetInspector kafkaDlqOffsetInspector(KafkaAdmin kafkaAdmin, KafkaDlqProperties properties) {
		return new AdminDltOffsetInspector(kafkaAdmin, properties.getAdminTimeout());
	}

	@Bean
	@ConditionalOnMissingBean
	DltReprocessingManager kafkaDlqReprocessingManager(KafkaDlqProperties properties,
			BeanFactory beanFactory, DltOffsetInspector offsetInspector,
			ObjectProvider<DltReprocessingCondition> conditions, ObjectProvider<DltRecordFilter> filters,
			ApplicationEventPublisher eventPublisher) {

		// not exposed as a bean: a TaskScheduler bean would replace the one Spring Boot provides for @Scheduled
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(2);
		scheduler.setThreadNamePrefix("kafka-dlq-");
		scheduler.setDaemon(true);
		return new DltReprocessingManager(properties.resolveTopics(),
				() -> beanFactory.getBean(KafkaListenerEndpointRegistry.class),
				() -> beanFactory.getBean(RetryTopicBeanNames.DESTINATION_TOPIC_RESOLVER_BEAN_NAME,
						DestinationTopicContainer.class),
				offsetInspector, new DltRecordPublisher(properties.getPublishTimeout()),
				conditions.orderedStream().toList(), filters.orderedStream().toList(), eventPublisher, scheduler,
				Clock.systemDefaultZone());
	}

	@Bean
	@ConditionalOnProperty(prefix = "kafka.dlq", name = "auto-create-parking-lot-topics", matchIfMissing = true)
	KafkaAdmin.NewTopics kafkaDlqParkingLotTopics(KafkaDlqProperties properties) {
		return new KafkaAdmin.NewTopics(properties.resolveTopics().stream().map(DltTopicSettings::parkingLotTopic)
				.filter(Objects::nonNull).map(topic -> TopicBuilder.name(topic).build()).toArray(NewTopic[]::new));
	}

}
