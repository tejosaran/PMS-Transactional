package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;
import com.pms.transactional.mapper.TradeMapper;
import com.pms.transactional.mapper.TransactionMapper;

import jakarta.transaction.Transactional;

@Service
public class TransactionService {
    @Autowired
    private TransactionDao transactionDao;

    @Autowired
    private TransactionMapper transactionMapper;

    @Autowired
    private TradeMapper tradeMapper;

    @Autowired
    private TradesDao tradesDao;

    @Autowired
    private OutboxEventsDao outboxEventsDao;

    @Transactional
    public void handleBuy(TradeProto trade) {

        UUID tradeId = UUID.fromString(trade.getTradeId());
        TradesEntity buyTrade = new TradesEntity();
        buyTrade.setTradeId(tradeId);
        buyTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        buyTrade.setSymbol(trade.getSymbol());
        buyTrade.setSide(TradeSide.BUY);
        buyTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        buyTrade.setQuantity(trade.getQuantity());
        buyTrade.setTimestamp(LocalDateTime.now());

        buyTrade = tradesDao.save(buyTrade); 

        TransactionsEntity buyTxn = new TransactionsEntity();
        buyTxn.setTrade(buyTrade); 
        buyTxn.setBuyPrice(null);
        buyTxn.setSellPrice(null);
        buyTxn.setSellQuantity(null);
        buyTxn.setRemainingQuantity(trade.getQuantity());

        transactionDao.save(buyTxn);

        TransactionProto transactionProto = transactionMapper.toProto(buyTxn);

        OutboxEventEntity outboxEventEntity = new OutboxEventEntity();
        outboxEventEntity.setAggregateId(buyTxn.getTransactionId());
        outboxEventEntity.setPayload(transactionProto.toByteArray());
        outboxEventEntity.setStatus("PENDING");
        outboxEventEntity.setCreatedAt(LocalDateTime.now());

        outboxEventsDao.save(outboxEventEntity);
    }

    @Transactional
    public void handleSell(TradeProto trade) {

        UUID tradeId = UUID.fromString(trade.getTradeId());
        TradesEntity sellTrade = new TradesEntity();
        sellTrade.setTradeId(tradeId);
        sellTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        sellTrade.setSymbol(trade.getSymbol());
        sellTrade.setSide(TradeSide.SELL);
        sellTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        sellTrade.setQuantity(trade.getQuantity());
        sellTrade.setTimestamp(LocalDateTime.now());

        sellTrade = tradesDao.save(sellTrade); 

        List<TransactionsEntity> buyList = transactionDao.findBuyOrdersFIFO(sellTrade.getPortfolioId(), sellTrade.getSymbol(),TradeSide.valueOf("BUY"));
        long sellQty = sellTrade.getQuantity();
        long totalAvailable = buyList.stream()
                .mapToLong(TransactionsEntity::getRemainingQuantity)
                .sum();

        if (totalAvailable < sellQty) {
            throw new RuntimeException(
                    "Insufficient BUY quantity. Required: " + sellQty + ", Available: " + totalAvailable);
        }

        for (TransactionsEntity buyTx : buyList) {

            if (sellQty == 0)
                break;

            long available = buyTx.getRemainingQuantity();
            long matchedQty = Math.min(available, sellQty);

            buyTx.setRemainingQuantity(available - matchedQty);
            transactionDao.save(buyTx);

            TransactionsEntity sellTxn = new TransactionsEntity();

            sellTxn.setTrade(sellTrade); 
            sellTxn.setBuyPrice(buyTx.getTrade().getPricePerStock());
            sellTxn.setSellPrice(sellTrade.getPricePerStock()); 
            sellTxn.setSellQuantity(matchedQty);
            sellTxn.setRemainingQuantity(null);

            transactionDao.save(sellTxn);

            sellQty -= matchedQty;

            TransactionProto transactionProto = transactionMapper.toProto(sellTxn);

            OutboxEventEntity outboxEventEntity = new OutboxEventEntity();
            outboxEventEntity.setAggregateId(sellTxn.getTransactionId());
            outboxEventEntity.setPayload(transactionProto.toByteArray());
            outboxEventEntity.setStatus("PENDING");
            outboxEventEntity.setCreatedAt(LocalDateTime.now());

            outboxEventsDao.save(outboxEventEntity);

        }

    }

}