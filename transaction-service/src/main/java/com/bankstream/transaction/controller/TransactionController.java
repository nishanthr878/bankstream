package com.bankstream.transaction.controller;

import com.bankstream.transaction.domain.Transaction;
import com.bankstream.transaction.domain.TransactionType;
import com.bankstream.transaction.service.TransactionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping
    public ResponseEntity<Transaction> initiateTransaction(
            @RequestBody InitiateTransactionRequest request) {

        Transaction transaction = transactionService.initiateTransaction(
                request.accountId(),
                request.amount(),
                request.type(),
                request.description()
        );

        return ResponseEntity.ok(transaction);
    }

    // Java 17 record — immutable request DTO, no boilerplate
    public record InitiateTransactionRequest(
            UUID accountId,
            BigDecimal amount,
            TransactionType type,
            String description
    ) {}
}