package com.pms.transactional.entities;

import java.math.BigDecimal;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;

public class OutboxEvents{
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "transaction_outbox_id")
    private UUID transactionOutboxId;

    @Column(name = "transaction_id")
    private UUID transactionId;

    @ManyToOne
    @JoinColumn(name = "trade_id")
    private TradesEntity trade;

    @Column(name = "buy_price", precision = 19, scale = 4)
    private BigDecimal buyPrice;

    @Column(name = "sell_price", precision = 19, scale = 4)
    private BigDecimal sellPrice;

    @Column(name = "remaining_quantity", nullable = false)
    private long remainingQuantity;

    @Column(name = "status", nullable = false)
    private String status;

}