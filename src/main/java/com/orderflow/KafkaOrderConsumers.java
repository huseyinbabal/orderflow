package com.orderflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.orderflow.avro.OrderEvent;

/** The Kafka side of the order backbone. Listener signatures use the GENERATED
 *  Avro type — everything else (groups, keys, offsets) is unchanged from Session 1.
 *
 *  orderflow.listeners.kafka=false turns these off, which is how the API pods run:
 *  in Kubernetes the consumers live in their OWN deployment (orderflow-consumer)
 *  so they scale independently of the HTTP intake. */
@Component
class KafkaOrderConsumers {

    private static final Logger log = LoggerFactory.getLogger(KafkaOrderConsumers.class);

    @KafkaListener(topics = "orders", groupId = "fulfillment",
            autoStartup = "${orderflow.listeners.kafka:true}")
    public void fulfillment(OrderEvent event) throws InterruptedException {
        Thread.sleep(20);   // simulate work — makes consumer lag visible under k6 load
        log.info("fulfillment processing {}", event.getOrderId());
    }

    @KafkaListener(topics = "orders", groupId = "analytics",
            autoStartup = "${orderflow.listeners.kafka:true}")
    public void analytics(OrderEvent event) {
        log.info("analytics counting {}", event.getOrderId());
    }
}
