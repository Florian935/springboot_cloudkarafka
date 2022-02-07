package com.florian935.cloud.kafka.consumer.consumer;

import com.florian935.cloud.kafka.consumer.domain.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProductConsumer {

    @KafkaListener(
            topics = "${spring.kafka.consumer.demo-topic-id}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "productKafkaListenerContainerFactory"
    )
    public void consume(@Payload Product product, ConsumerRecordMetadata metadata) {

        log.info("Received message\n---\nTOPIC: {}; PARTITION: {}; OFFSET: {};\nPAYLOAD: {}\n---",
                metadata.topic(), metadata.partition(), metadata.offset(), product);
    }
}
