package com.bankstream.fraud.streams;

import com.bankstream.account.event.avro.AccountEvent;
import com.bankstream.fraud.config.KafkaStreamsConfig;
import com.bankstream.fraud.event.avro.FraudAlertEvent;
import com.bankstream.fraud.model.AccountTier;
import com.bankstream.transaction.event.avro.TransactionInitiatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;


import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDetectionTopology {

    private static final String TRANSACTION_TOPIC = "transaction.initiated";
    private static final String ACCOUNT_TOPIC     = "account.created";
    private static final String FRAUD_ALERT_TOPIC = "fraud.alert";

    // Velocity window — sliding, 5 minutes
    private static final Duration VELOCITY_WINDOW   = Duration.ofMinutes(5);
    // Rapid large spend window — tumbling, 1 hour
    private static final Duration SPEND_WINDOW      = Duration.ofHours(1);
    // Grace period for late events
    private static final Duration GRACE_PERIOD      = Duration.ofSeconds(30);

    private final KafkaStreamsConfig streamsConfig;

    // @Autowired on a method with StreamsBuilder parameter
    // Spring Kafka calls this method after creating the StreamsBuilder bean
    // This is where you define the topology
    @Autowired
    public void buildTopology(StreamsBuilder builder) {

        var transactionSerde = streamsConfig.<TransactionInitiatedEvent>avroSerde();
        var accountSerde     = streamsConfig.<AccountEvent>avroSerde();
        var fraudAlertSerde  = streamsConfig.<FraudAlertEvent>avroSerde();

        // GlobalKTable — account data
        GlobalKTable<String, AccountEvent> accountTable = builder.globalTable(
                ACCOUNT_TOPIC,
                Consumed.with(Serdes.String(), accountSerde)
        );

        // Input stream — keyed by accountId
        KStream<String, TransactionInitiatedEvent> transactions = builder.stream(
                TRANSACTION_TOPIC,
                Consumed.with(Serdes.String(), transactionSerde)
        );

        // ── Rule 2: Velocity — on RAW transactions (no account data needed) ────
        transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
                        VELOCITY_WINDOW, GRACE_PERIOD))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(
                                "velocity-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .filter((windowedKey, count) -> count != null && count >= 3)
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key(),
                        FraudAlertEvent.newBuilder()
                                .setAlertId(UUID.randomUUID().toString())
                                .setAccountId(windowedKey.key())
                                .setTransactionId("MULTIPLE")
                                .setAlertType("VELOCITY")
                                .setReason(count + " transactions in 5-minute window")
                                .setAmount(0.0)
                                .setAccountTier("UNKNOWN")
                                .setDetectedAt(Instant.now().toEpochMilli())
                                .build()
                ))
                .to(FRAUD_ALERT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));

        // ── Rule 3: Rapid large spend — on RAW transactions ────────────────────
        transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(SPEND_WINDOW, GRACE_PERIOD))
                .aggregate(
                        () -> 0.0,
                        (accountId, transaction, total) -> total + transaction.getAmount(),
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as(
                                        "hourly-spend-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()))
                .toStream()
                .filter((windowedKey, total) -> total != null && total > 1_00_000.0)
                .map((windowedKey, total) -> KeyValue.pair(
                        windowedKey.key(),
                        FraudAlertEvent.newBuilder()
                                .setAlertId(UUID.randomUUID().toString())
                                .setAccountId(windowedKey.key())
                                .setTransactionId("MULTIPLE")
                                .setAlertType("RAPID_LARGE")
                                .setReason(String.format("Total spend %.2f exceeds limit", total))
                                .setAmount(total)
                                .setAccountTier("UNKNOWN")
                                .setDetectedAt(Instant.now().toEpochMilli())
                                .build()
                ))
                .to(FRAUD_ALERT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));

        // ── Enrich: join with account table ────────────────────────────────────
        KStream<String, EnrichedTransaction> enriched = transactions.join(
                accountTable,
                (key, transaction) -> transaction.getAccountId(),
                (transaction, account) -> new EnrichedTransaction(transaction, account)
        );

        // ── Rule 1: High value — needs account tier from enrichment ────────────
        enriched
                .filter((accountId, et) -> {
                    double threshold = AccountTier
                            .fromString(et.account().getTier())
                            .getHighValueThreshold();
                    return et.transaction().getAmount() > threshold;
                })
                .map((accountId, et) -> KeyValue.pair(
                        accountId,
                        buildAlert(
                                et.transaction(), et.account(),
                                "HIGH_VALUE",
                                String.format("Amount %.2f exceeds tier threshold",
                                        et.transaction().getAmount())
                        )
                ))
                .to(FRAUD_ALERT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));

        log.info("Fraud detection topology built");
    }

    // ── Helper: build FraudAlertEvent ───────────────────────────────────────
    private FraudAlertEvent buildAlert(TransactionInitiatedEvent tx,
                                       AccountEvent account,
                                       String alertType,
                                       String reason) {
        return FraudAlertEvent.newBuilder()
                .setAlertId(UUID.randomUUID().toString())
                .setAccountId(tx.getAccountId())
                .setTransactionId(tx.getTransactionId())
                .setAlertType(alertType)
                .setReason(reason)
                .setAmount(tx.getAmount())
                .setAccountTier(account.getTier())
                .setDetectedAt(Instant.now().toEpochMilli())
                .build();
    }

    // ── Inner class: enriched transaction ───────────────────────────────────
    // Holds transaction + account together after the GlobalKTable join
    // Not an Avro class — internal to the topology only
    record EnrichedTransaction(
            TransactionInitiatedEvent transaction,
            AccountEvent account
    ) {}
}