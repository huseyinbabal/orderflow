package com.orderflow.es;

import java.time.Instant;
import java.util.UUID;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Path B — one row of the event store. Append-only. There is no "orders" table
 *  on this path: an order's state is the fold of its events, and
 *  {@code UNIQUE (aggregate_id, version)} is the optimistic-concurrency guard —
 *  two writers cannot both append version N. */
@Entity
@Table(name = "es_event")
class EsEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long seq;

    private UUID aggregateId;
    private int version;
    private String type;                  // OrderOpened, ItemAdded, OrderCheckedOut

    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;

    private Instant at;

    protected EsEvent() {}

    EsEvent(UUID aggregateId, int version, String type, String payload) {
        this.aggregateId = aggregateId;
        this.version = version;
        this.type = type;
        this.payload = payload;
        this.at = Instant.now();
    }

    Long seq() { return seq; }
    UUID aggregateId() { return aggregateId; }
    int version() { return version; }
    String type() { return type; }
    String payload() { return payload; }
    Instant at() { return at; }
}
