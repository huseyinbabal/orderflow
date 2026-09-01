package com.orderflow;

import java.util.List;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class NotificationTopology {

    static final List<String> CHANNELS = List.of("email", "sms", "push");

    @Bean
    TopicExchange notifyExchange() {
        return new TopicExchange("notify.topic");
    }

    @Bean
    DirectExchange notifyDlx() {
        return new DirectExchange("notify.dlx");
    }

    @Bean
    Declarables notificationQueues(TopicExchange notifyExchange, DirectExchange notifyDlx) {
        var declarables = new java.util.ArrayList<org.springframework.amqp.core.Declarable>();
        for (String ch : CHANNELS) {
            Queue queue = QueueBuilder.durable("notify." + ch)
                    .quorum()                                          // replicated + delivery counting
                    .withArgument("x-delivery-limit", 5)               // backstop: 5 tries → DLX
                    .deadLetterExchange("notify.dlx")
                    .deadLetterRoutingKey(ch)
                    .build();
            Queue dlq = QueueBuilder.durable("notify." + ch + ".dlq").quorum().build();
            Binding binding = BindingBuilder.bind(queue).to(notifyExchange).with("notify.*");
            Binding dlqBinding = BindingBuilder.bind(dlq).to(notifyDlx).with(ch);
            declarables.addAll(List.of(queue, dlq, binding, dlqBinding));
        }
        return new Declarables(declarables);
    }
}
