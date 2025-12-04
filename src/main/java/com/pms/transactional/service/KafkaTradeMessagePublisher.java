package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;

@Service
public class KafkaTradeMessagePublisher {

    // @Qualifier("tradeKafkaTemplate")
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishTradeMessage(String key, TradeProto trade) {
        System.out.println("Hi from publisher");

        kafkaTemplate.send("validatedtrades-topic", key, trade) // ✔ FIXED
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        System.out.println("Kafka Offset: " + res.getRecordMetadata());
                    } else {
                        System.out.println("Failed to publish message: " + ex.getMessage());
                    }
                });
    }

}
