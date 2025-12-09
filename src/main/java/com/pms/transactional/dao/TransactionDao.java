package com.pms.transactional.dao;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;

public interface TransactionDao extends JpaRepository<TransactionsEntity, UUID>{
    @Query("""
                SELECT tx FROM TransactionsEntity tx
                JOIN TradesEntity t ON tx.trade.tradeId = t.tradeId
                WHERE t.side = :side  
                  AND tx.quantity > 0
                  AND t.portfolioId = :pid
                  AND t.symbol = :symbol
                ORDER BY t.timestamp ASC
            """)
    List<TransactionsEntity> findBuyOrdersFIFO(UUID pid, String symbol,TradeSide side);
}