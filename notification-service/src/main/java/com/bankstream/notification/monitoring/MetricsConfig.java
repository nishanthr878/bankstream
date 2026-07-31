package com.bankstream.notification.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter notificationSentCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.notifications.sent.total")
                .description("Total notification sent successfully")
                .register(registry);
    }

    @Bean
    public Counter dlqRoutedCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.dlq.routed.total")
                .description("Total message routed to DLQ")
                .register(registry);
    }

    @Bean
    public Counter duplicateEventsCounter(MeterRegistry registry) {
        return Counter.builder("bankstream.events.duplicate.total")
                .description("Total duplicate events skipped")
                .register(registry);
    }


}
