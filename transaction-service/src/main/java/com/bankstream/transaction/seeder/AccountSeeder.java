package com.bankstream.transaction.seeder;

import com.bankstream.account.event.avro.AccountEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class AccountSeeder implements ApplicationRunner {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    private static final String TOPIC = "account.created";

    // Seed data matching init.sql
    private static final List<AccountEvent> SEED_ACCOUNTS = List.of(
            buildAccount(
                    "a0000001-0000-0000-0000-000000000001",
                    "ACC001", "Nishanth Kumar", "PREMIUM", "ACTIVE"),
            buildAccount(
                    "a0000001-0000-0000-0000-000000000002",
                    "ACC002", "Ravi Shankar", "STANDARD", "ACTIVE"),
            buildAccount(
                    "a0000001-0000-0000-0000-000000000003",
                    "ACC003", "Priya Mehta", "ELITE", "ACTIVE")
    );

    @Override
    public void run(ApplicationArguments args) {
        log.info("Seeding {} accounts to topic {}", SEED_ACCOUNTS.size(), TOPIC);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);

        try (KafkaProducer<String, AccountEvent> producer =
                     new KafkaProducer<>(props)) {

            for (AccountEvent account : SEED_ACCOUNTS) {
                ProducerRecord<String, AccountEvent> record =
                        new ProducerRecord<>(TOPIC, account.getAccountId(), account);

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to seed account {}: {}",
                                account.getAccountId(), ex.getMessage());
                    } else {
                        log.info("Seeded account {} to partition {} offset {}",
                                account.getAccountId(),
                                metadata.partition(),
                                metadata.offset());
                    }
                });
            }
            producer.flush();
            log.info("Account seeding complete");
        }
    }

    private static AccountEvent buildAccount(String id, String number,
                                             String name, String tier,
                                             String status) {
        return AccountEvent.newBuilder()
                .setAccountId(id)
                .setAccountNumber(number)
                .setOwnerName(name)
                .setTier(tier)
                .setStatus(status)
                .build();
    }
}