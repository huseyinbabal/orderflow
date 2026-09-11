package com.orderflow;

import java.time.Duration;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

/** The XREADGROUP side. A consumer-group listener on the "feed" stream:
 *
 *   XGROUP CREATE feed dashboard 0 MKSTREAM     (once, idempotent)
 *   XREADGROUP GROUP dashboard <pod> COUNT 100 BLOCK 2s STREAMS feed >
 *   XACK feed dashboard <id>                    (after apply — leaves the PEL)
 *
 *  One consumer per pod. Scale the orderflow-consumer deployment and the group
 *  protocol splits the stream across them — competing consumers, same as Kafka.
 *
 *  orderflow.listeners.redis=false turns this (and the sweeper) off — that's how
 *  the API pods run: they only XADD, they don't consume. */
@Component
class FeedConsumer {

    private static final Logger log = LoggerFactory.getLogger(FeedConsumer.class);

    static final String STREAM = FeedPublisher.STREAM;
    static final String GROUP  = "dashboard";

    private final RedisConnectionFactory connectionFactory;
    private final StringRedisTemplate redis;
    private final boolean enabled;
    private final String consumerName;

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;

    FeedConsumer(RedisConnectionFactory connectionFactory,
                 StringRedisTemplate redis,
                 @Value("${orderflow.listeners.redis:true}") boolean enabled,
                 @Value("${HOSTNAME:feed-local}") String consumerName) {
        this.connectionFactory = connectionFactory;
        this.redis = redis;
        this.enabled = enabled;
        this.consumerName = consumerName;
    }

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        if (!enabled) {
            log.info("feed consumer disabled (orderflow.listeners.redis=false) — XADD only");
            return;
        }
        ensureGroup();

        var options = StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofSeconds(2))                       // BLOCK 2000
                .batchSize(100)                                           // COUNT 100 — batching is the win
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        container = StreamMessageListenerContainer.create(connectionFactory, options);
        // receive() — NOT receiveAutoAck(): a handler that throws (or a pod that dies)
        // leaves the entry in the PEL. That is the whole point of the sweeper demo.
        container.receive(
                Consumer.from(GROUP, consumerName),
                StreamOffset.create(STREAM, ReadOffset.lastConsumed()),   // ">"
                this::onMessage);
        container.start();
        log.info("feed consumer {} joined group {} on stream {}", consumerName, GROUP, STREAM);
    }

    private void ensureGroup() {
        try {
            redis.opsForStream().createGroup(STREAM, ReadOffset.from("0"), GROUP);
            log.info("created consumer group {} on {}", GROUP, STREAM);
        } catch (DataAccessException busygroup) {
            // BUSYGROUP: the group already exists — every other pod hits this. Fine.
        }
    }

    void onMessage(MapRecord<String, String, String> msg) {
        FeedStats.record(redis, msg.getValue());                 // idempotent aggregation
        redis.opsForStream().acknowledge(GROUP, msg);            // XACK feed dashboard <id>
        log.debug("feed applied {}", msg.getId());
    }

    @PreDestroy
    void stop() {
        if (container != null) {
            container.stop();
        }
    }
}
