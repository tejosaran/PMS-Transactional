package com.pms.transactional.entities;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

import com.pms.transactional.enums.TradeSide;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Entity
@Table(name = "transactions")
public class TransactionsEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID transactionId;
    private UUID tradeId;
    private UUID portfolioId;
    private String symbol;
    private TradeSide side;
    private BigDecimal buyPrice;
    private BigDecimal sellPrice;
    private long quantity;
    private long remainingQuantity;
    private LocalDateTime timestamp;
}