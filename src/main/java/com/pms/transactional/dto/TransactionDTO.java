package com.pms.transactional.dto;

import java.math.BigDecimal;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TransactionDTO{
    private UUID transactionId;
    private BigDecimal buyPrice;
    private BigDecimal sellPrice;
    private long remainingQuantity;
    private long sellQuantity;
    private TradeDTO trade;
}