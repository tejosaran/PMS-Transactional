package com.pms.transactional.dao;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;

public interface TransactionDao extends JpaRepository<TransactionsEntity, UUID>{
    @Query("""
                SELECT tx FROM TransactionsEntity tx
                JOIN TradesEntity t ON tx.trade.tradeId = t.tradeId
                WHERE t.side = :side  
                  AND tx.quantity > 0
                  AND t.portfolioId IN :pids
                  AND t.symbol IN :symbols
                ORDER BY t.timestamp ASC
            """)
    List<TransactionsEntity> findEligibleBuys(@Param("pids")List<UUID> pids, @Param("symbols")List<String> symbols,@Param("side")TradeSide side);
} 