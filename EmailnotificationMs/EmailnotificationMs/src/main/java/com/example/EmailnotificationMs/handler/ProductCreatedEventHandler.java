package com.example.EmailnotificationMs.handler;

import lombok.extern.slf4j.Slf4j;
import org.kafkaeventcore.coreeventslib.events.ProductCreatedEvent;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic")
@Slf4j
public class ProductCreatedEventHandler {

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        log.info("Received an event:: " + productCreatedEvent.getTitle());
    }
}
