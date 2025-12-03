package com.pms.transactional.entities;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import com.pms.transactional.enums.TradeSide;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
    @Column(name = "trade_id", nullable = false)
    private UUID tradeId;

    @Column(name = "portfolio_id", nullable = false)
    private UUID portfolioId;

    @Column(name = "symbol", nullable = false, length = 255)
    private String symbol;

    @Enumerated(EnumType.ORDINAL)
    @Column(name = "side", nullable = false, length = 50)
    private TradeSide side;

    @Column(name = "price_per_stock", nullable = false, precision = 19, scale = 4)
    private BigDecimal pricePerStock;

    @Column(name = "quantity", nullable = false)
    private long quantity;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;
}