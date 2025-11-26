package com.pms.transactional.entities;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.pms.transactional.enums.TradeSide;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TransactionsEntity{
    private UUID transactionId; 
    private UUID tradeId;
    private UUID portfolioId;
    private String cusipId;
    private TradeSide side;
    private BigDecimal buyPrice;
    private BigDecimal sellPrice;
    private long quantity;
    private float remainingQuantity;
    private Instant timestamp;
}