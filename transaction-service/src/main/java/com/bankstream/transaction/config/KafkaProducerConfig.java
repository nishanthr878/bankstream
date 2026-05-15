package com.bankstream.transaction.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Broker addresses — both brokers for redundancy
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Wait for ALL ISR replicas to acknowledge
        // Paired with min.insync.replicas on broker side
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry on transient failures (network blips, leader election)
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Deduplicate retries — broker tracks sequence numbers per producer
        // Prevents duplicate messages when producer retries after timeout
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Max in-flight requests per connection
        // Must be <= 5 when idempotence is enabled
        // Higher values break sequence number ordering guarantees
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Batch up to 32KB before sending to broker
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        // Wait 5ms to fill batch — better throughput, slight latency tradeoff
        // Acceptable for banking events (not millisecond-critical)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // Key is always a String (account_id)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value serialized as JSON
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}