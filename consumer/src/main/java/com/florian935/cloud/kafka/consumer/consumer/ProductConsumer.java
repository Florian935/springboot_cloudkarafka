package com.florian935.cloud.kafka.consumer.consumer;

import com.florian935.cloud.kafka.consumer.domain.Product;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class ProductConsumer {

    @KafkaListener(
            topics = "${spring.kafka.consumer.demo-topic-id}",
            groupId = "${spring.kafka.consumer.group-id}",
            topicPartitions =  {
                    @TopicPartition(
                        topic = "${spring.kafka.consumer.demo-topic-id}",
                        partitionOffsets = {
                                @PartitionOffset(partition = "0", initialOffset = "0"),
                                @PartitionOffset(partition = "1", initialOffset = "0"),
                                @PartitionOffset(partition = "2", initialOffset = "0")
                        }
            ) },
            containerFactory = "productKafkaListenerContainerFactory"
    )
    public void consume(@Payload Product product,
                        @Headers Map<String, Object> headers,
                        ConsumerRecordMetadata metadata,
                        ConsumerRecord<String, Object> record) {
        log.info("\nReceived message\n---\nTOPIC: {}; PARTITION: {}; OFFSET: {};\nPAYLOAD: {}\n---",
                metadata.topic(), metadata.partition(), metadata.offset(), product);

        System.out.println("============ HEADERS =============");
        System.out.println(headers);
        System.out.println("=========================");

        System.out.println("============ CONSUMER RECORD =============");
        System.out.println(record);
        System.out.println("=========================");
    }
}
