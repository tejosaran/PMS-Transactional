package com.pms.transactional.dao;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;

@Repository
public interface TransactionDao extends JpaRepository<TransactionsEntity, UUID>{

    @Modifying
    @Query(value =  """
                      INSERT INTO transactions (transaction_id, trade_id, buy_price, quantity)
                      VALUES (:#{#txn.transactionId}, :#{#txn.trade.tradeId}, :#{#txn.buyPrice}, :#{#txn.quantity})
                      ON CONFLICT (transaction_id) DO NOTHING
                    """, nativeQuery = true)
    void upsert(@Param("txn") TransactionsEntity txn);

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