package com.pms.transactional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.pms.transactional.TransactionProto;

@Service
public class KafkaMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @KafkaListener(topics = "transactions-topic", groupId = "transactions", containerFactory = "kafkaListenerContainerFactory")
    public void consume1(TransactionProto transaction) {
        try {
            logger.info("Consumer message (parsed): {}", transaction);
        } catch (Exception e) {
            logger.error("Failed to parse protobuf", e);
        }
    }

    // @KafkaListener(topics = "transactions-topic", groupId = "transactions",
    // containerFactory = "kafkaListenerContainerFactory")
    // public void consume2(TransactionsEntity transactions) {
    // logger.info("Consumer2 message: " + transactions.toString());
    // }

    // @KafkaListener(topics = "transactions-topic", groupId = "transactions",
    // containerFactory = "kafkaListenerContainerFactory")
    // public void consume3(TransactionsEntity transactions) {
    // logger.info("Consumer 3 message: " + transactions.toString());
    // }

    // @KafkaListener(topics = "transactions-topic", groupId = "transactions",
    // containerFactory = "kafkaListenerContainerFactory")
    // public void consume4(TransactionsEntity transactions) {
    // logger.info("Consumer 4 message: " + transactions.toString());
    // }
}
