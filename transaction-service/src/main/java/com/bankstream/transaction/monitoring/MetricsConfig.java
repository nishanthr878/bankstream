package com.bankstream.transaction.monitoring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {
    // Counts total transactions initiated
    @Bean("transactionInitiatedCounter")
    public Counter transationInitiatedCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.transactions.initiated.total")
                .description("Total number of transactions initiated")
                .register(registry);
    }

    // Counts outbox entries published successfully
    @Bean("outboxPublishedCounter")
    public Counter outboxPublishedCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.outbox.published.total")
                .description("Total outbox entries published to kafka")
                .register(registry);
    }

    // Counts outbox published failures
    @Bean("outboxFailedCounter")
    public Counter outboxFailedCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.outbox.failed.total")
                .description("Total outbox entires that failed to publish")
                .register(registry);
    }

    // Tracks transaction process time
    @Bean
    public Timer transactionTimer(MeterRegistry registry) {
        return Timer.builder("bankstream.transactions.duration")
                .description("Time to process a transaction request")
                .publishPercentileHistogram(true)  // ← add this
                .publishPercentiles(0.5, 0.95, 0.99)  // ← add this
                .register(registry);
    }

}
