package com.orderflow.outbox;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cdc")
class OutboxController {

    private final OutboxOrderService service;

    OutboxController(OutboxOrderService service) {
        this.service = service;
    }

    record PlaceOrder(String customerId, BigDecimal amount) {}

    @PostMapping("/orders")
    Map<String, UUID> place(@RequestBody PlaceOrder req) {
        return Map.of("orderId", service.placeOrder(req.customerId(), req.amount()));
    }

    /** What the app wrote — compare this with what lands on the Kafka topic. */
    @GetMapping("/outbox")
    List<Map<String, Object>> outbox() {
        return service.recentOutbox();
    }
}
