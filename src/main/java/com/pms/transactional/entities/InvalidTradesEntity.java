package com.pms.transactional.entities;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Entity
@Table(name="invalid_trades")
public class InvalidTradesEntity {
   
    @Id
    @Column(name = "invalid_trade_id", nullable = false)
    private UUID invalidTradeId;

    @Column(name="aggregate_id")
    private UUID aggregateId;

    @Column(name = "payload")
    private byte[] payload;

    @Column(name="error_message")
    private String errorMessage;

}
