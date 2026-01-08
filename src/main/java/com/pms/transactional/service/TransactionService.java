package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.BatchInsertDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;
import com.pms.transactional.exceptions.InvalidTradeException;
import com.pms.transactional.mapper.TransactionMapper;

import jakarta.transaction.Transactional;

@Service
public class TransactionService {

    @Autowired
    private TransactionDao transactionDao;

    @Autowired
    private BatchInsertDao batchInsertDao;

    @Autowired
    private TransactionMapper transactionMapper;

    Logger logger = LoggerFactory.getLogger(TransactionService.class);

    @Transactional
    public void processBuyBatch(List<TradeProto> buyBatch) {
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> transactions = new ArrayList<>();
        List<OutboxEventEntity> outboxEvents = new ArrayList<>();

        for (TradeProto record : buyBatch) {
            processBuy(record, trades, transactions, outboxEvents);
        }

        batchInsertDao.batchInsertTrades(trades);
        batchInsertDao.batchInsertTransactions(transactions);
        batchInsertDao.batchInsertOutboxEvents(outboxEvents);
        
        System.out.println("Buy Batch Flushed: Trades=" + trades.size() + " Transactions=" + transactions.size() + " Outbox=" + outboxEvents.size());
    }

    @Transactional
    public void processSellBatch(List<TradeProto> sellBatch) {
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> transactions = new ArrayList<>();
        List<OutboxEventEntity> outboxEvents = new ArrayList<>();
        List<TransactionsEntity> updatedBuys = new ArrayList<>();
        List<InvalidTradesEntity> invalidTrades = new ArrayList<>();

        Set<UUID> portfolioIds = sellBatch.stream()
                .map(record -> UUID.fromString(record.getPortfolioId()))
                .collect(Collectors.toSet());

        Set<String> symbols = sellBatch.stream()
                .map(record -> record.getSymbol())
                .collect(Collectors.toSet());

        List<TransactionsEntity> eligibleBuys = transactionDao.findEligibleBuys(new ArrayList<>(portfolioIds),
                new ArrayList<>(symbols), TradeSide.BUY);

        Map<String, List<TransactionsEntity>> buyMap = eligibleBuys.stream()
                .collect(Collectors.groupingBy(b -> b.getTrade().getPortfolioId() + "_" + b.getTrade().getSymbol(),
                        LinkedHashMap::new, Collectors.toList()));

        for (TradeProto record : sellBatch){   
            try {
                processSell(record, buyMap, updatedBuys, trades, transactions, outboxEvents,invalidTrades);
            } catch (InvalidTradeException ex) {
                logger.info("Invalid trade message detected");
                handleInvalid(record, invalidTrades, ex.getErrorMessage());
            }
        }

        batchInsertDao.batchInsertTrades(trades);
        batchInsertDao.batchInsertTransactions(transactions);
        batchInsertDao.batchInsertOutboxEvents(outboxEvents);

        if (!updatedBuys.isEmpty()) {
            batchInsertDao.batchUpdateBuyQuantities(updatedBuys);
        }

        if (!invalidTrades.isEmpty()) {
            batchInsertDao.batchInsertInvalidTrades(invalidTrades);
        }
        System.out.println("Sell Batch Flushed: Trades=" + trades.size() + " Transactions=" + transactions.size() + " Outbox=" + outboxEvents.size());
    }

    public void processBuy(TradeProto trade, List<TradesEntity> trades, List<TransactionsEntity> txns,
            List<OutboxEventEntity> outbox) {

        UUID tradeId = UUID.fromString(trade.getTradeId());

        TradesEntity buyTrade = new TradesEntity();
        buyTrade.setTradeId(tradeId);
        buyTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        buyTrade.setSymbol(trade.getSymbol());
        buyTrade.setSide(TradeSide.BUY);
        buyTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        buyTrade.setQuantity(trade.getQuantity());
        buyTrade.setTimestamp(LocalDateTime.ofInstant(
                Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),
                ZoneOffset.UTC));
        trades.add(buyTrade);

        TransactionsEntity buyTxn = new TransactionsEntity();
        String key = "BUY_" + trade.getTradeId();
        UUID txnId = UUID.nameUUIDFromBytes(key.getBytes());
        buyTxn.setTransactionId(txnId);
        buyTxn.setTrade(buyTrade);
        buyTxn.setBuyPrice(null);
        buyTxn.setQuantity(trade.getQuantity());
        txns.add(buyTxn);

        TransactionProto transactionProto = transactionMapper.toProto(buyTxn);
        OutboxEventEntity event = new OutboxEventEntity();
        String outboxKey = transactionProto.getTransactionId();
        event.setTransactionOutboxId(UUID.nameUUIDFromBytes(outboxKey.getBytes()));
        event.setAggregateId(buyTxn.getTransactionId());
        event.setPayload(transactionProto.toByteArray());
        event.setPortfolioId(UUID.fromString(transactionProto.getPortfolioId()));
        event.setStatus("PENDING");

        event.setCreatedAt(LocalDateTime.now());
        outbox.add(event);
        
    }

    public void processSell(TradeProto trade, Map<String, List<TransactionsEntity>> allBuys,
        List<TransactionsEntity> updatedBuys, List<TradesEntity> trades, List<TransactionsEntity> txns,
        List<OutboxEventEntity> outbox,List<InvalidTradesEntity> invalidTrades) {
        
        try {
            UUID tradeId = UUID.fromString(trade.getTradeId());
            UUID portfolioId = UUID.fromString(trade.getPortfolioId());
            long qtyToSell = trade.getQuantity();
            LocalDateTime sellTimestamp = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),
                    ZoneOffset.UTC);

            List<TransactionsEntity> inventory = allBuys.get(portfolioId + "_" + trade.getSymbol());

            if(inventory == null || inventory.isEmpty()){
                throw new InvalidTradeException("No eligible buys for SELL " + tradeId);
            }
            long totalAvailable = 0;
            int lastIndexToProcess = -1;

            for (int i = 0; i < inventory.size(); i++) {
                TransactionsEntity buy = inventory.get(i);
                if (buy.getQuantity() > 0 && buy.getTrade().getTimestamp().isBefore(sellTimestamp)) {
                    totalAvailable += buy.getQuantity();
                    if (totalAvailable >= qtyToSell) {
                        lastIndexToProcess = i; 
                        break;
                    }
                }
            }

            if (lastIndexToProcess == -1) {
                throw new InvalidTradeException("Insufficient quantity for tradeId " + tradeId + ". Found: " + totalAvailable);
            }

            TradesEntity sellTrade = new TradesEntity();
            sellTrade.setTradeId(tradeId);
            sellTrade.setPortfolioId(portfolioId);
            sellTrade.setSymbol(trade.getSymbol());
            sellTrade.setSide(TradeSide.SELL);
            sellTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
            sellTrade.setQuantity(trade.getQuantity());
            sellTrade.setTimestamp(sellTimestamp);
            trades.add(sellTrade);

            long remainingToMatch = qtyToSell;

            for(int i = 0; i <= lastIndexToProcess; i++){
                TransactionsEntity buyTx = inventory.get(i);
                if (buyTx.getQuantity() <= 0 || !buyTx.getTrade().getTimestamp().isBefore(sellTimestamp)) {
                    continue;
                }
                long matchedQty = Math.min(buyTx.getQuantity(), remainingToMatch);

                buyTx.setQuantity(buyTx.getQuantity() - matchedQty);
                updatedBuys.add(buyTx);

                TransactionsEntity sellTxn = new TransactionsEntity();
                String txnKey = "SELL_" + tradeId + "BUY_" + buyTx.getTransactionId();
                sellTxn.setTransactionId(UUID.nameUUIDFromBytes(txnKey.getBytes()));
                sellTxn.setTrade(sellTrade);
                sellTxn.setBuyPrice(buyTx.getTrade().getPricePerStock());
                sellTxn.setQuantity(matchedQty);
                txns.add(sellTxn);

                TransactionProto proto = transactionMapper.toProto(sellTxn);
                OutboxEventEntity event = new OutboxEventEntity();
                event.setTransactionOutboxId(UUID.nameUUIDFromBytes(proto.getTransactionId().getBytes()));
                event.setAggregateId(sellTxn.getTransactionId());
                event.setPortfolioId(portfolioId);
                event.setPayload(proto.toByteArray());
                event.setStatus("PENDING");
                event.setCreatedAt(LocalDateTime.now());
                outbox.add(event);

                remainingToMatch -= matchedQty;
                if(remainingToMatch <= 0) break;
            }
        } 
        catch(InvalidTradeException exception){
            handleInvalid(trade, invalidTrades, exception.getErrorMessage());
        }
    }

    public void handleInvalid(TradeProto trade, List<InvalidTradesEntity> invalidTrades, String errorMessage) {
        InvalidTradesEntity invalidTrade = new InvalidTradesEntity();
        String key = trade.getTradeId();
        invalidTrade.setInvalidTradeId(UUID.nameUUIDFromBytes(key.getBytes()));
        invalidTrade.setAggregateId(UUID.fromString(trade.getTradeId()));
        invalidTrade.setPayload(trade.toByteArray());   
        invalidTrade.setErrorMessage(errorMessage);

        invalidTrades.add(invalidTrade);
    }

}