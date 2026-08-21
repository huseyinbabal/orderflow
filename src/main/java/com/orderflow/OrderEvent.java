package com.orderflow;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public record OrderEvent(String eventId, String orderId,
                         String customerId, BigDecimal amount, Instant at) {

    public static OrderEvent of(CreateOrder req) {
        return new OrderEvent(UUID.randomUUID().toString(), req.orderId(),
                req.customerId(), req.amount(), Instant.now());
    }

    public static OrderEvent random() {
        var rnd = ThreadLocalRandom.current();
        return new OrderEvent(UUID.randomUUID().toString(),
                "o-" + rnd.nextInt(1_000_000),
                "c-" + rnd.nextInt(10_000),
                BigDecimal.valueOf(rnd.nextDouble(5, 500)).setScale(2, java.math.RoundingMode.HALF_UP),
                Instant.now());
    }
}
