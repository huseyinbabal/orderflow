package com.orderflow.es;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/es")
class EsController {

    private final EsOrderService service;

    EsController(EsOrderService service) {
        this.service = service;
    }

    record Open(String customerId) {}
    record Item(String sku, BigDecimal price) {}

    @PostMapping("/orders")
    Map<String, UUID> open(@RequestBody Open req) {
        return Map.of("orderId", service.open(req.customerId()));
    }

    @PostMapping("/orders/{id}/items")
    void addItem(@PathVariable UUID id, @RequestBody Item req) {
        service.addItem(id, req.sku(), req.price());
    }

    @PostMapping("/orders/{id}/checkout")
    void checkout(@PathVariable UUID id) {
        service.checkout(id);
    }

    /** Current state — rebuilt from the event log on every call. */
    @GetMapping("/orders/{id}")
    Map<String, Object> state(@PathVariable UUID id) {
        return service.state(id);
    }

    /** The event log itself — this is the source of truth. */
    @GetMapping("/orders/{id}/history")
    List<Map<String, Object>> history(@PathVariable UUID id) {
        return service.history(id);
    }
}
