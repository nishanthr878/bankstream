package com.bankstream.notification.service;

import com.bankstream.transaction.event.avro.TransactionInitiatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationService {

    public void sendTransactionNotification(TransactionInitiatedEvent event) {
        double amount = event.getAmount();

        // Simulate failure for amounts ending in .99 — for DLQ testing
        if (String.valueOf(amount).endsWith(".99")) {
            throw new RuntimeException(
                    "Simulated notification failure for amount: " + amount
            );
        }

        log.info("Notification sent: Account {} transaction for amount {} {}",
                event.getAccountId(), event.getCurrency(), event.getAmount());
    }
}