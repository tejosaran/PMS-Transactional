package com.pms.transactional.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;

@Repository
public class BatchInsertDao {
    
    @Autowired
    JdbcTemplate jdbcTemplate;

    public void batchInsertTrades(List<TradesEntity> batchTrades){
        String insertQuery = " INSERT INTO TRADES(trade_id,portfolio_id,symbol,side,price_per_stock,quantity,timestamp) " + 
                            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(trade_id) DO NOTHING ";

        jdbcTemplate.batchUpdate(insertQuery,batchTrades,500,(preparedStatement,trade)->{
            preparedStatement.setObject(1,trade.getTradeId());
            preparedStatement.setObject(2,trade.getPortfolioId());
            preparedStatement.setString(3, trade.getSymbol());
            preparedStatement.setString(4, trade.getSide().name());
            preparedStatement.setBigDecimal(5, trade.getPricePerStock());
            preparedStatement.setLong(6, trade.getQuantity());
            preparedStatement.setObject(7, trade.getTimestamp());
        });
    }

    public void batchInsertTransactions(List<TransactionsEntity> batchTransactions){
        String insertQuery = " INSERT INTO TRANSACTIONS(transaction_id,buy_price,quantity,trade_id) " + 
                            "VALUES (?,?,?,?) ON CONFLICT(transaction_id) DO NOTHING ";

        jdbcTemplate.batchUpdate(insertQuery,batchTransactions,500,(preparedStatement,transaction)->{
            preparedStatement.setObject(1,transaction.getTransactionId());
            preparedStatement.setObject(4,transaction.getTrade().getTradeId());
            preparedStatement.setBigDecimal(2, transaction.getBuyPrice());
            preparedStatement.setLong(3, transaction.getQuantity());
        });
    }

    public void batchInsertOutboxEvents(List<OutboxEventEntity> batchOutboxEventEntities){
        String insertQuery = " INSERT INTO OUTBOX_EVENTS(transaction_outbox_id,aggregate_id,portfolio_id,payload,status,created_at) " + 
                            "VALUES (?,?,?,?,?,?) ON CONFLICT(transaction_outbox_id) DO NOTHING ";

        jdbcTemplate.batchUpdate(insertQuery,batchOutboxEventEntities,500,(preparedStatement,outboxEvent)->{
            preparedStatement.setObject(1,outboxEvent.getTransactionOutboxId());
            preparedStatement.setObject(2,outboxEvent.getAggregateId());
            preparedStatement.setObject(3, outboxEvent.getPortfolioId());
            preparedStatement.setBytes(4, outboxEvent.getPayload());
            preparedStatement.setString(5, outboxEvent.getStatus());
            preparedStatement.setObject(6, outboxEvent.getCreatedAt());
        });
    }

    public void batchInsertInvalidTrades(List<InvalidTradesEntity> invalidTrades){
        String insertQuery = "INSERT INTO INVALID_TRADES(invalid_trade_id,aggregate_id,payload,errorMessage) " +
                             "VALUES(?,?,?,?) ON CONFLICT(aggregate_id) DO NOTHING";

        jdbcTemplate.batchUpdate(insertQuery,invalidTrades,500,(preparedStatement,invalidTrade)->{
            preparedStatement.setObject(1, invalidTrade.getInvalidTradeId());
            preparedStatement.setObject(2, invalidTrade.getAggregateId());
            preparedStatement.setBytes(3, invalidTrade.getPayload());
            preparedStatement.setString(4, invalidTrade.getErrorMessage());
        });
    }

    public void batchUpdateBuyQuantities(List<TransactionsEntity> updatedBuys) {
        String updateQuery = "UPDATE transactions SET quantity = ? WHERE transaction_id = ?";
        jdbcTemplate.batchUpdate(updateQuery, updatedBuys, 500, (preparedStatement, transaction) -> {
            preparedStatement.setLong(1, transaction.getQuantity());
            preparedStatement.setObject(2, transaction.getTransactionId());
        });
    }
}
