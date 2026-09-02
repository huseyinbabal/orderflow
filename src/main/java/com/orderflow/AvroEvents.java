package com.orderflow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import com.orderflow.avro.OrderEvent;

/** Builds instances of the GENERATED Avro class (com.orderflow.avro.OrderEvent).
 *  The Kafka path speaks Avro; the RabbitMQ path keeps the plain JSON record. */
final class AvroEvents {

    private AvroEvents() {}

    static OrderEvent of(CreateOrder req) {
        return OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(req.orderId())
                .setCustomerId(req.customerId())
                .setAmount(req.amount().setScale(2, RoundingMode.HALF_UP))
                .setAt(Instant.now())
                .build();
    }

    static OrderEvent random() {
        var rnd = ThreadLocalRandom.current();
        return OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId("o-" + rnd.nextInt(1_000_000))
                .setCustomerId("c-" + rnd.nextInt(10_000))
                .setAmount(BigDecimal.valueOf(rnd.nextDouble(5, 500)).setScale(2, RoundingMode.HALF_UP))
                .setAt(Instant.now())
                .build();
    }
}
