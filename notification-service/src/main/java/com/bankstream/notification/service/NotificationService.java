package com.bankstream.notification.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class NotificationService {

    // Simulates sending a notification (email, SMS, push)
    // In production this calls an email provider, SMS gateway etc.
    // We simulate failure for certain amounts to test retry + DLQ
    public void sendTransactionNotification(Map<String, Object> event) {
        Object amount = event.get("amount");
        Object accountId = event.get("accountId");

        // Simulate failure for amounts ending in .99 — for testing DLQ
        if (amount != null && amount.toString().endsWith(".99")) {
            throw new RuntimeException(
                    "Simulated notification failure for amount: " + amount
            );
        }

        // Happy path — log the notification
        log.info("Notification sent: Account {} transaction for amount {}",
                accountId, amount);
    }
}
