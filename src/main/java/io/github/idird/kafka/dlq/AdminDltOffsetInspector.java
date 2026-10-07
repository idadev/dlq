package io.github.idird.kafka.dlq;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * {@link DltOffsetInspector} based on the Kafka admin client.
 */
public class AdminDltOffsetInspector implements DltOffsetInspector {

	private final Supplier<Admin> adminSupplier;

	private final Duration timeout;

	public AdminDltOffsetInspector(KafkaAdmin kafkaAdmin, Duration timeout) {
		this(() -> Admin.create(kafkaAdmin.getConfigurationProperties()), timeout);
	}

	public AdminDltOffsetInspector(Supplier<Admin> adminSupplier, Duration timeout) {
		this.adminSupplier = adminSupplier;
		this.timeout = timeout;
	}

	@Override
	public DltOffsets inspect(String groupId, String topic) {
		try (Admin admin = this.adminSupplier.get()) {
			TopicDescription description = await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
			List<TopicPartition> partitions = description.partitions().stream()
					.map(partition -> new TopicPartition(topic, partition.partition())).toList();
			Map<TopicPartition, Long> earliest = offsets(admin, partitions, OffsetSpec.earliest());
			Map<TopicPartition, Long> end = offsets(admin, partitions, OffsetSpec.latest());
			Map<TopicPartition, OffsetAndMetadata> committed = await(
					admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());

			Map<TopicPartition, Long> positions = new HashMap<>();
			for (TopicPartition partition : partitions) {
				OffsetAndMetadata offset = committed.get(partition);
				long start = earliest.getOrDefault(partition, 0L);
				// a group without committed offset starts from the beginning, see DltReprocessingManager
				positions.put(partition, offset != null ? Math.max(offset.offset(), start) : start);
			}
			return new DltOffsets(positions, end);
		}
		catch (ExecutionException ex) {
			if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
				return DltOffsets.empty();
			}
			throw new KafkaException("Failed to read the offsets of " + topic + " for group " + groupId, ex.getCause());
		}
		catch (TimeoutException ex) {
			throw new KafkaException("Timed out reading the offsets of " + topic + " for group " + groupId, ex);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new KafkaException("Interrupted while reading the offsets of " + topic, ex);
		}
	}

	private Map<TopicPartition, Long> offsets(Admin admin, Collection<TopicPartition> partitions, OffsetSpec spec)
			throws ExecutionException, InterruptedException, TimeoutException {

		return await(admin.listOffsets(partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> spec)))
				.all()).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
	}

	private <T> T await(org.apache.kafka.common.KafkaFuture<T> future)
			throws ExecutionException, InterruptedException, TimeoutException {

		return future.get(this.timeout.toMillis(), TimeUnit.MILLISECONDS);
	}

}
