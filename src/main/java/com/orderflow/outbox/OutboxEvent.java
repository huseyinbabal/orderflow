package com.orderflow.outbox;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Path A — the outbox row. Written in the SAME transaction as {@link OrderRow},
 *  so there is no dual-write: one commit, both rows, or neither. Debezium tails
 *  this table's WAL and the EventRouter SMT turns each row into a Kafka message
 *  on {@code outbox.event.<aggregate_type>}. */
@Entity
@Table(name = "outbox")
class OutboxEvent {

    @Id
    private UUID id;
    @Column(name = "aggregate_type")
    private String aggregateType;          // topic routing: outbox.event.Order
    @Column(name = "aggregate_id")
    private String aggregateId;            // Kafka message key
    private String type;                   // OrderPlaced, OrderCancelled, …
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;                // event body — verbatim on the wire
    @Column(name = "created_at")
    private Instant createdAt;

    protected OutboxEvent() {}

    OutboxEvent(UUID id, String aggregateType, String aggregateId,
                String type, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.type = type;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    UUID id() { return id; }
    String aggregateType() { return aggregateType; }
    String aggregateId() { return aggregateId; }
    String type() { return type; }
    String payload() { return payload; }
    Instant createdAt() { return createdAt; }
}
