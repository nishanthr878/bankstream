package com.bankstream.fraud.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams  // tells Spring to manage Kafka Streams lifecycle
public class KafkaStreamsConfig {

    @Value("${kafka.streams.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.streams.application-id}")
    private String applicationId;

    @Value("${kafka.streams.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${kafka.streams.state-dir}")
    private String stateDir;

    // Spring looks for a bean named exactly this — it's the default streams config
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();

        // Core Kafka Streams config
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Application ID — also used as consumer group ID for this streams app
        // All instances with same application ID form one streams cluster
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        // Default key serde — String for partition keys (accountId)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        // Default value serde — SpecificAvroSerde for our generated Avro classes
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                SpecificAvroSerde.class);

        // Schema Registry URL — used by SpecificAvroSerde for ser/de
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);

        // Where RocksDB stores state (aggregations, windows)
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // Exactly-once semantics — atomic read + process + write
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2);

        // How often to commit state (default 30s — lower for faster recovery)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        return new KafkaStreamsConfiguration(props);
    }

    // Serde factory method — reused across topology
    // Each call creates a new SpecificAvroSerde configured for this schema registry
    public <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> avroSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ), false); // false = value serde, not key serde
        return serde;
    }
}