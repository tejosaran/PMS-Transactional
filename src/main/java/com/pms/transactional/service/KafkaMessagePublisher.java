package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.TransactionProto;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishMessage(String key, TransactionProto transaction) {
        System.out.println("Hi from publisher");
        kafkaTemplate.send("transactions-topic",key,transaction)
                .whenComplete((res,ex)->{
                    if (ex == null){
                        System.out.println("Kafka Offset: " + res.getRecordMetadata());
                    }
                });
    }

}
