package com.orderflow;

import java.util.stream.IntStream;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class OrderController {

    private final RabbitTemplate rabbit;
    private final KafkaTemplate<String, OrderEvent> kafka;

    OrderController(RabbitTemplate rabbit, KafkaTemplate<String, OrderEvent> kafka) {
        this.rabbit = rabbit;
        this.kafka = kafka;
    }

    @PostMapping("/orders")
    void create(@RequestBody CreateOrder req) {
        var event = OrderEvent.of(req);
        rabbit.convertAndSend("orders", event);        // default exchange → queue "orders"
        kafka.send("orders", event.orderId(), event);  // key → same order, same partition
        // one publish → the broker copies it into every queue bound with "notify.*"
        rabbit.convertAndSend("notify.topic", "notify.all", event);
    }

    @PostMapping("/orders/burst/{count}")
    void burst(@PathVariable int count, @RequestParam(defaultValue = "rabbit") String broker) {
        IntStream.range(0, count).parallel().forEach(i -> {
            var event = OrderEvent.random();
            switch (broker) {
                case "rabbit" -> rabbit.convertAndSend("orders", event);
                case "kafka" -> kafka.send("orders", event.orderId(), event);
                // Black Friday: one publish → email+sms+push queues via notify.* bindings
                case "notify" -> rabbit.convertAndSend("notify.topic", "notify.all", event);
                default -> throw new IllegalArgumentException("broker must be rabbit, kafka or notify");
            }
        });
    }
}
