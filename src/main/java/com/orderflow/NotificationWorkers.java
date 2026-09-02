package com.orderflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
class NotificationWorkers {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorkers.class);

    @RabbitListener(queues = "notify.email", concurrency = "2-8",
            autoStartup = "${orderflow.listeners.rabbit:true}")
    public void email(OrderEvent e) throws InterruptedException {
        Thread.sleep(200);                              // email provider: slow
        log.info("EMAIL sent for {}", e.orderId());
    }

    @RabbitListener(queues = "notify.sms", concurrency = "2-4",
            autoStartup = "${orderflow.listeners.rabbit:true}")
    public void sms(OrderEvent e) throws InterruptedException {
        Thread.sleep(100);
        // ~10% of orders hit a "broken" provider — deterministic by orderId, so the
        // same message fails every retry and ends up in the DLQ (the class demo)
        if (Math.abs(e.orderId().hashCode()) % 10 == 0)
            throw new IllegalStateException("SMS provider 503 for " + e.orderId());
        log.info("SMS sent for {}", e.orderId());
    }

    @RabbitListener(queues = "notify.push", concurrency = "4-16",
            autoStartup = "${orderflow.listeners.rabbit:true}")
    public void push(OrderEvent e) {
        log.info("PUSH sent for {}", e.orderId());
    }
}
