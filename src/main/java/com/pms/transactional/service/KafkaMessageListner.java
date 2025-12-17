package com.pms.transactional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.pms.transactional.TransactionProto;

import jakarta.transaction.Transactional;

@Service
public class KafkaMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @Transactional
    @KafkaListener(topics = "transactions-topic", groupId = "transactions", containerFactory = "kafkaListenerContainerFactory")
    public void consume1(TransactionProto transaction) {
        try {
            logger.info("Consumer message (parsed): {}", transaction);
        } catch (Exception e) {
            logger.error("Failed to parse protobuf ", e);
        }
    }

    
}
