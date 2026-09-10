package com.orderflow.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Path A — the business row. Just the current state of an order; the history
 *  lives on Kafka, put there by Debezium reading the {@link OutboxEvent} table. */
@Entity
@Table(name = "orders")
class OrderRow {

    @Id
    private UUID id;
    @Column(name = "customer_id")
    private String customerId;
    private BigDecimal amount;
    private String status;
    @Column(name = "created_at")
    private Instant createdAt;

    protected OrderRow() {}

    OrderRow(UUID id, String customerId, BigDecimal amount, String status, Instant createdAt) {
        this.id = id;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.createdAt = createdAt;
    }

    UUID id() { return id; }
    String customerId() { return customerId; }
    BigDecimal amount() { return amount; }
    String status() { return status; }
    Instant createdAt() { return createdAt; }
}
