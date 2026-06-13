package com.demo.orders;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

@Configuration
public class RabbitMQConfig {

    @Bean
    public Queue orderSagaQueue() {
        return new Queue("order_saga_queue", true);
    }

    @Bean
    public Binding bindingSagaExchange(Queue orderSagaQueue, TopicExchange sagaExchange) {
        return BindingBuilder.bind(orderSagaQueue).to(sagaExchange).with("inventory.*");
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
