package com.pms.transactional.dto;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class OutboxEventsDTO{
    private UUID transactionOutboxId;
    private TransactionDTO transaction;
    private String status;
}