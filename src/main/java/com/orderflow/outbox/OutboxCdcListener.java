package com.orderflow.outbox;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

/** Proof the outbox row actually became a Kafka message. The app's normal
 *  consumer factory speaks Avro (Session 3); Debezium writes plain JSON, so this
 *  listener gets its own String factory. */
@Configuration
class OutboxCdcConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> stringKafkaListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        var cf = new DefaultKafkaConsumerFactory<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(cf);
        return factory;
    }
}

@Component
class OutboxCdcListener {

    private static final Logger log = LoggerFactory.getLogger(OutboxCdcListener.class);

    @KafkaListener(topics = "outbox.event.Order", groupId = "cdc-demo",
            containerFactory = "stringKafkaListenerContainerFactory",
            autoStartup = "${orderflow.listeners.cdc:true}")
    void onOrderEvent(String value) {
        log.info("CDC ⇒ outbox.event.Order  {}", value);
    }
}
