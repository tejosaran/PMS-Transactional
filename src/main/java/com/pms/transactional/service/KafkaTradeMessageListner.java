package com.pms.transactional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @KafkaListener(topics = "validatedtrades-topic", groupId = "trades", containerFactory = "tradekafkaListenerContainerFactory")
    public void consume2(TradeProto trade) {
        try {
            logger.info("Consumer message (parsed): {}", trade);
        } catch (Exception e) {
            logger.error("Failed to parse protobuf", e);
        }
    }

}
