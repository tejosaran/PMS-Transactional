package com.pms.transactional.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.pms.transactional.TransactionProto;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    // public void publishMessage(ValidatedTrade trade) {
    // try {
    // String portfolioKey = trade.getTrade().getPortfolioId().toString();
    // CompletableFuture<SendResult<String, Object>> future =
    // kafkaTemplate.send("transactions-topic",
    // portfolioKey,
    // transactions);
    // future.whenComplete((result, ex) -> {
    // if (ex != null) {
    // System.out.println("Failed to send message: " + ex.getMessage());
    // } else {
    // System.out.println("Message sent successfully to topic: " +
    // result.getRecordMetadata().topic());
    // }
    // });

    // //
    // kafkaTemplate.send("transactions-topic",message.getPortfolioId().toString(),
    // // message);
    // } catch (Exception e) {
    // System.out.println("Exception in publishing message: " + e.getMessage());
    // }
    // }

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
