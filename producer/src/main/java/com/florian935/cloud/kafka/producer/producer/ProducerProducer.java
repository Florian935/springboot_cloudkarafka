package com.florian935.cloud.kafka.producer.producer;

import com.florian935.cloud.kafka.producer.domain.Product;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static lombok.AccessLevel.PRIVATE;

@Service
@FieldDefaults(level = PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public class ProducerProducer {

    KafkaTemplate<String, Object> kafkaTemplate;

    public void send(Product product) {

        this.kafkaTemplate.send("gr62pzpd-demo", product);
        System.out.println("Sent sample message [" + product + "] to gr62pzpd-demo");
    }
}
