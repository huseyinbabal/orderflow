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
    private final KafkaTemplate<String, com.orderflow.avro.OrderEvent> kafka;

    OrderController(RabbitTemplate rabbit, KafkaTemplate<String, com.orderflow.avro.OrderEvent> kafka) {
        this.rabbit = rabbit;
        this.kafka = kafka;
    }

    // ?broker=kafka lets the k6 test load ONE broker without the others muddying the graphs
    @PostMapping("/orders")
    void create(@RequestBody CreateOrder req, @RequestParam(defaultValue = "all") String broker) {
        if (broker.equals("all") || broker.equals("rabbit")) {
            var event = OrderEvent.of(req);
            rabbit.convertAndSend("orders", event);        // default exchange → queue "orders"
            // one publish → the broker copies it into every queue bound with "notify.*"
            if (broker.equals("all")) rabbit.convertAndSend("notify.topic", "notify.all", event);
        }
        if (broker.equals("all") || broker.equals("kafka")) {
            var avro = AvroEvents.of(req);                 // the GENERATED class — typed contract
            kafka.send("orders", avro.getOrderId(), avro); // key → same order, same partition
        }
    }

    @PostMapping("/orders/burst/{count}")
    void burst(@PathVariable int count, @RequestParam(defaultValue = "rabbit") String broker) {
        IntStream.range(0, count).parallel().forEach(i -> {
            switch (broker) {
                case "rabbit" -> rabbit.convertAndSend("orders", OrderEvent.random());
                case "kafka" -> {
                    var avro = AvroEvents.random();
                    kafka.send("orders", avro.getOrderId(), avro);
                }
                // hot-partition demo: every message keyed by the SAME whale customer
                case "whale" -> {
                    var avro = AvroEvents.random();
                    kafka.send("orders", "whale-1", avro);
                }
                // Black Friday: one publish → email+sms+push queues via notify.* bindings
                case "notify" -> rabbit.convertAndSend("notify.topic", "notify.all", OrderEvent.random());
                default -> throw new IllegalArgumentException("broker must be rabbit, kafka, whale or notify");
            }
        });
    }
}
