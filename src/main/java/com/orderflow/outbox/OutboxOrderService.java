package com.orderflow.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.orderflow.JsonSupport;

/** Path A — CDC + Transactional Outbox.
 *
 *  {@link #placeOrder} writes the business row AND the outbox row in one
 *  transaction. That is the fix for the dual-write bug: we never "save to DB,
 *  then publish to Kafka" — a crash between those two leaves them disagreeing.
 *  Here both rows commit atomically, and Debezium turns the outbox row into a
 *  Kafka message afterwards, at-least-once, on its own. */
@Service
public class OutboxOrderService {

    private final OrderRowRepository orders;
    private final OutboxRepository outbox;
    private final JsonSupport json;

    OutboxOrderService(OrderRowRepository orders, OutboxRepository outbox, JsonSupport json) {
        this.orders = orders;
        this.outbox = outbox;
        this.json = json;
    }

    @Transactional
    public UUID placeOrder(String customerId, BigDecimal amount) {
        var id = UUID.randomUUID();
        var now = Instant.now();

        orders.save(new OrderRow(id, customerId, amount, "PLACED", now));

        String payload = json.write(Map.of(
                "orderId", id.toString(),
                "customerId", customerId,
                "amount", amount,
                "placedAt", now.toString()));
        outbox.save(new OutboxEvent(UUID.randomUUID(), "Order", id.toString(),
                "OrderPlaced", payload, now));

        return id;                       // both rows commit together — or neither does
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> recentOutbox() {
        return outbox.findByOrderByCreatedAtDesc(Limit.of(20)).stream()
                .map(e -> Map.<String, Object>of(
                        "id", e.id(),
                        "type", e.type(),
                        "aggregateId", e.aggregateId(),
                        "payload", json.read(e.payload()),
                        "createdAt", e.createdAt()))
                .toList();
    }
}
