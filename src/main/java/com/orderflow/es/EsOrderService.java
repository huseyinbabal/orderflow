package com.orderflow.es;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.orderflow.JsonSupport;

/** Path B — Event Sourcing.
 *
 *  Every command: load the event log, rebuild state, check the invariant, append
 *  the new event(s). No UPDATE, ever. The DB's {@code UNIQUE (aggregate_id,
 *  version)} makes concurrent appends of the same version fail loudly. */
@Service
public class EsOrderService {

    private final EsEventRepository store;
    private final JsonSupport json;

    EsOrderService(EsEventRepository store, JsonSupport json) {
        this.store = store;
        this.json = json;
    }

    @Transactional
    public UUID open(String customerId) {
        var id = UUID.randomUUID();
        append(id, 1, "OrderOpened", Map.of("customerId", customerId));
        return id;
    }

    @Transactional
    public void addItem(UUID id, String sku, BigDecimal price) {
        var agg = load(id);
        if (!"OPEN".equals(agg.status)) {
            throw new IllegalStateException("order " + id + " is " + agg.status + ", not OPEN");
        }
        append(id, agg.version + 1, "ItemAdded", Map.of("sku", sku, "price", price));
    }

    @Transactional
    public void checkout(UUID id) {
        var agg = load(id);
        if (agg.items.isEmpty()) {
            throw new IllegalStateException("order " + id + " has no items");
        }
        append(id, agg.version + 1, "OrderCheckedOut", Map.of("total", agg.total));
    }

    @Transactional(readOnly = true)
    public Map<String, Object> state(UUID id) {
        var agg = load(id);
        return Map.of(
                "orderId", agg.id,
                "customerId", agg.customerId,
                "items", agg.items,
                "total", agg.total,
                "status", agg.status,
                "version", agg.version);
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> history(UUID id) {
        return store.findByAggregateIdOrderByVersionAsc(id).stream()
                .map(e -> Map.<String, Object>of(
                        "version", e.version(),
                        "type", e.type(),
                        "payload", json.read(e.payload()),
                        "at", e.at()))
                .toList();
    }

    private OrderAggregate load(UUID id) {
        var history = store.findByAggregateIdOrderByVersionAsc(id);
        if (history.isEmpty()) {
            throw new NoSuchElementException("no such order " + id);
        }
        return OrderAggregate.rebuild(history, json);
    }

    private void append(UUID id, int version, String type, Map<String, ?> payload) {
        // saveAndFlush so a UNIQUE(aggregate_id, version) clash surfaces here, in this call
        store.saveAndFlush(new EsEvent(id, version, type, json.write(payload)));
    }
}
