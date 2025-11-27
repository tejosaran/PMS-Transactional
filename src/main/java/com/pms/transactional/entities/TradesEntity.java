package com.pms.transactional.entities;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

import com.pms.transactional.enums.TradeSide;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Entity
@Table(name = "trades")
public class TradesEntity {

    @Id
    private UUID tradeId;
    private UUID portfolioId;
    private String symbol;
    private TradeSide side;
    private BigDecimal unitPrice;
    private long quantity;
    private LocalDateTime timestamp;
}