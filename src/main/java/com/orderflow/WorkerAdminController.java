package com.orderflow;

import java.util.Map;

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// The backpressure lever from the slides: stop consuming without stopping the app.
// Intake (HTTP + publishes) keeps working; queues absorb the load until resume.
@RestController
@RequestMapping("/admin/workers")
class WorkerAdminController {

    private final RabbitListenerEndpointRegistry registry;

    WorkerAdminController(RabbitListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @PostMapping("/pause")
    Map<String, Object> pause() {
        registry.getListenerContainers().forEach(c -> c.stop());
        return status();
    }

    @PostMapping("/resume")
    Map<String, Object> resume() {
        registry.getListenerContainers().forEach(c -> c.start());
        return status();
    }

    private Map<String, Object> status() {
        long running = registry.getListenerContainers().stream().filter(c -> c.isRunning()).count();
        return Map.of("containers", registry.getListenerContainers().size(), "running", running);
    }
}
