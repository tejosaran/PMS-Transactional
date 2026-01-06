package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.entities.OutboxEventEntity;

@Component
public class OutboxEventProcessor {

    private KafkaTemplate<String, Object> kafkaTemplate;

    public OutboxEventProcessor(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public ProcessingResult process(List<OutboxEventEntity> events) {

        List<UUID> successfulIds = new ArrayList<>();

        for (OutboxEventEntity event : events) {
            try {
                TransactionProto proto = TransactionProto.parseFrom(event.getPayload());

                kafkaTemplate.send(
                        "transactions-topic",
                        event.getPortfolioId().toString(),
                        proto);

                successfulIds.add(event.getTransactionOutboxId());

            } catch (InvalidProtocolBufferException e) {
                return ProcessingResult.poisonPill(successfulIds, event);
            } catch (Exception e) {
                return ProcessingResult.systemFailure(successfulIds);
            }
        }

        return ProcessingResult.success(successfulIds);
    }
}
