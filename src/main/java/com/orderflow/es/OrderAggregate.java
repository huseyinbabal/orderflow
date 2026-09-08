package com.orderflow.es;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.orderflow.JsonSupport;

/** Path B — the aggregate. Not stored anywhere: {@link #rebuild} folds the event
 *  log into current state every time. {@code version} is the version of the last
 *  event applied — the next command appends {@code version + 1}. */
class OrderAggregate {

    UUID id;
    String customerId;
    final List<String> items = new ArrayList<>();
    BigDecimal total = BigDecimal.ZERO;
    String status = "NEW";
    int version = 0;

    static OrderAggregate rebuild(List<EsEvent> history, JsonSupport json) {
        var agg = new OrderAggregate();
        for (EsEvent e : history) {
            agg.apply(e, json);
        }
        return agg;
    }

    private void apply(EsEvent e, JsonSupport json) {
        var body = json.read(e.payload());
        switch (e.type()) {
            case "OrderOpened" -> {
                this.id = e.aggregateId();
                this.customerId = body.get("customerId").asString();
                this.status = "OPEN";
            }
            case "ItemAdded" -> {
                this.items.add(body.get("sku").asString());
                this.total = this.total.add(body.get("price").decimalValue());
            }
            case "OrderCheckedOut" -> this.status = "CHECKED_OUT";
            default -> throw new IllegalStateException("unknown event type " + e.type());
        }
        this.version = e.version();
    }
}
