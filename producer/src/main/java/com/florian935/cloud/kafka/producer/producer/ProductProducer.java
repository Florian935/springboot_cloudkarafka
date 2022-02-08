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
public class ProductProducer {

    KafkaTemplate<String, Object> kafkaTemplate;

    public void send(Product product, String action) {

        switch (action) {
            case "create" -> {
                kafkaTemplate.send("YOUR_TOPIC_NAME", 0, "create", product);
                System.out.println("[CREATE] Sent sample message [" + product + "] to OUR_TOPIC_NAME");
            }
            case "delete" -> {
                kafkaTemplate.send("OUR_TOPIC_NAME", 1, "delete", product);
                System.out.println("[DELETE] Sent sample message [" + product + "] to OUR_TOPIC_NAME");
            }
            case "update" -> {
                kafkaTemplate.send("OUR_TOPIC_NAME", 2, "update", product);
                System.out.println("[UPDATE] Sent sample message [" + product + "] to OUR_TOPIC_NAME");
            }
            default -> {
                kafkaTemplate.send("OUR_TOPIC_NAME", 0, "fetch", product);
                System.out.println("[FETCH] Sent sample message [" + product + "] to OUR_TOPIC_NAME");
            }
        }


    }
}
