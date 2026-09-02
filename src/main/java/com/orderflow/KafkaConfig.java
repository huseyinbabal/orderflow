package com.orderflow;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
class KafkaConfig {

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name("orders").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic ordersDltTopic() {   // same partition count — the recoverer keeps rec.partition()
        return TopicBuilder.name("orders.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> avroTemplate,
                                     ProducerFactory<Object, Object> pf) {
        // A poison pill never deserialized — its raw byte[] needs a plain-bytes producer,
        // while records that failed IN the listener re-publish through the Avro template.
        var bytesTemplate = new KafkaTemplate<>(pf,
                Map.of("value.serializer", ByteArraySerializer.class));
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, avroTemplate);

        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (rec, ex) -> new TopicPartition(rec.topic() + ".DLT", rec.partition()));
        var backoff = new ExponentialBackOff(500, 2.0);   // 0.5s→1s→2s→4s, then DLT
        backoff.setMaxAttempts(4);
        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(SerializationException.class);  // poison: no retry
        return handler;
    }
}
