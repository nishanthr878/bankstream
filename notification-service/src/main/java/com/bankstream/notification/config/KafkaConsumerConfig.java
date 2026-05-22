package com.bankstream.notification.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // earliest: start from beginning if no committed offset exists
        // latest: start from now, skip old messages
        // earliest is correct for notification-service — we never want to miss a transaction
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Manual commit — we control exactly when offset is committed
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Max messages per poll — keep low so processing time stays under
        // max.poll.interval.ms and we don't get kicked out of the group
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // If poll() isn't called within this time, consumer is removed from group
        // 5 minutes default — we keep it lower to detect slow consumers faster
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // MANUAL_IMMEDIATE: offset committed immediately when acknowledgment is called
        // vs MANUAL: batches acks and commits at next poll
        // MANUAL_IMMEDIATE is safer — no risk of acking messages we haven't processed
        factory.getContainerProperties().setAckMode(
                ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );

        // Number of concurrent consumer threads
        // Should match partition count for maximum parallelism
        // We have 3 partitions so 3 concurrent consumers
        factory.setConcurrency(3);

        return factory;
    }
}