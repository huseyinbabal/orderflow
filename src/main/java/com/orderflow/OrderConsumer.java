package com.orderflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @RabbitListener(queues = "orders", autoStartup = "${orderflow.listeners.rabbit:true}")
    public void handle(OrderEvent event) throws InterruptedException {
        log.info("processing order {}", event.orderId());
        Thread.sleep(50);   // simulate work — this delay matters in the burst demo
    }
}
