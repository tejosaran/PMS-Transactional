package com.pms.transactional.mapper;

import java.time.LocalDateTime;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TransactionsEntity;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OutboxEventMapper{
    
    public OutboxEventEntity toEntity(
            TransactionsEntity txn,
            TransactionProto proto) {

        OutboxEventEntity entity = new OutboxEventEntity();
        entity.setAggregateId(txn.getTransactionId());
        entity.setPayload(proto.toByteArray());
        entity.setStatus("PENDING");
        entity.setCreatedAt(LocalDateTime.now());
        return entity;
    }
    
}