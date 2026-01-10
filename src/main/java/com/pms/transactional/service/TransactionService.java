package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.BatchInsertDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;
import com.pms.transactional.mapper.TransactionMapper;



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
    public void processUnifiedBatch(List<TradeProto> buyBatch, List<TradeProto> sellBatch) {
        List<TradesEntity> allTrades = new ArrayList<>();
        List<TransactionsEntity> allTransactions = new ArrayList<>();
        List<OutboxEventEntity> allOutboxEvents = new ArrayList<>();
        List<InvalidTradesEntity> allInvalidTrades = new ArrayList<>();
        Map<String, List<TransactionsEntity>> currentBatchInventory = new HashMap<>();
        Set<TransactionsEntity> modifiedDbBuys = new LinkedHashSet<>();
        if (!buyBatch.isEmpty()) {
            for (TradeProto newBuyTrade : buyBatch) {
                TransactionsEntity buyTx = processBuy(newBuyTrade, allTrades, allTransactions, allOutboxEvents);
                String inventoryKey = buyTx.getTrade().getPortfolioId() + "_" + buyTx.getTrade().getSymbol();
                currentBatchInventory.computeIfAbsent(inventoryKey, k -> new ArrayList<>()).add(buyTx);
            }
        }

        if (!sellBatch.isEmpty()){

            Set<UUID> portfolioIds = sellBatch.stream()
                .map(record -> UUID.fromString(record.getPortfolioId()))
                .collect(Collectors.toSet());

            Set<String> symbols = sellBatch.stream()
                .map(record -> record.getSymbol())
                .collect(Collectors.toSet());

            List<TransactionsEntity> eligibleBuys = transactionDao.findEligibleBuys(new ArrayList<>(portfolioIds),
                new ArrayList<>(symbols), TradeSide.BUY);
            

            for (TradeProto sellProto : sellBatch) {
                processSell(sellProto, eligibleBuys, currentBatchInventory, 
                                    allTrades, allTransactions, allOutboxEvents, modifiedDbBuys,allInvalidTrades);

            }
        }
            logger.info("Processing completed.. saving to database");
            
            if (!allTrades.isEmpty()) batchInsertDao.batchInsertTrades(allTrades);
            if (!allTransactions.isEmpty()) batchInsertDao.batchInsertTransactions(allTransactions);
            if (!allOutboxEvents.isEmpty()) batchInsertDao.batchInsertOutboxEvents(allOutboxEvents);

            if (!modifiedDbBuys.isEmpty()) {
                    batchInsertDao.batchUpdateBuyQuantities(new ArrayList<>(modifiedDbBuys));
                }
            logger.info("invalid trades started ...");
            if (!allInvalidTrades.isEmpty()) {
                batchInsertDao.batchInsertInvalidTrades(allInvalidTrades);
            }

            
        logger.info("Batch Processed: Trades={}, Transactions={}, Invalid={}", 
            allTrades.size(), allTransactions.size(), allInvalidTrades.size());
    }


    public TransactionsEntity processBuy(TradeProto trade, List<TradesEntity> trades, List<TransactionsEntity> transactions,List<OutboxEventEntity> outboxEvents) {

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
        transactions.add(buyTxn);

        TransactionProto transactionProto = transactionMapper.toProto(buyTxn);
        OutboxEventEntity event = new OutboxEventEntity();
        String outboxKey = transactionProto.getTransactionId();
        event.setTransactionOutboxId(UUID.nameUUIDFromBytes(outboxKey.getBytes()));
        event.setAggregateId(buyTxn.getTransactionId());
        event.setPayload(transactionProto.toByteArray());
        event.setPortfolioId(UUID.fromString(transactionProto.getPortfolioId()));
        event.setStatus("PENDING");

        event.setCreatedAt(LocalDateTime.now());
        outboxEvents.add(event);
        
        return buyTxn;
    }

    public void processSell(TradeProto trade,List<TransactionsEntity> dbInventory, Map<String, List<TransactionsEntity>> newBuyTransactions,List<TradesEntity> trades, List<TransactionsEntity> transactions,List<OutboxEventEntity> outboxEvents,Set<TransactionsEntity> modifiedDbBuys,List<InvalidTradesEntity> invalidTrades) {
    
        UUID tradeId = UUID.fromString(trade.getTradeId());
        UUID portfolioId = UUID.fromString(trade.getPortfolioId());
        long qtyToSell = trade.getQuantity();
        LocalDateTime sellTimestamp = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),
                ZoneOffset.UTC);
        String key = portfolioId +"_"+ trade.getSymbol();

        List<TransactionsEntity> dbTransactions = dbInventory.stream()
                                                        .filter(b -> (b.getTrade().getPortfolioId() + "_" + b.getTrade().getSymbol()).equals(key))
                                                        .collect(Collectors.toList());

        List<TransactionsEntity> currentBatchTransactions = newBuyTransactions.getOrDefault(key, Collections.emptyList());

        List<TransactionsEntity> inventory = new ArrayList<>();
        inventory.addAll(dbTransactions);
        inventory.addAll(currentBatchTransactions);
        inventory.sort(Comparator.comparing(b -> b.getTrade().getTimestamp()));

        if(inventory == null || inventory.isEmpty()){
            handleInvalid(trade, invalidTrades, "No eligible previous buys avaliable for this sell trade");
            return;
        }
        
        long totalAvailable = inventory.stream()
            .filter(b -> b.getQuantity() > 0 && b.getTrade().getTimestamp().isBefore(sellTimestamp))
            .mapToLong(TransactionsEntity::getQuantity).sum();

        if (totalAvailable < qtyToSell) {
            handleInvalid(trade, invalidTrades, "Insufficient quantity for " + key + ". Needed " + qtyToSell);
            return;
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

        for (TransactionsEntity buyTx : inventory) {
            if (buyTx.getQuantity() <= 0 || !buyTx.getTrade().getTimestamp().isBefore(sellTimestamp)) continue;

            long matched = Math.min(buyTx.getQuantity(), remainingToMatch);
            buyTx.setQuantity(buyTx.getQuantity() - matched);

            if (!transactions.contains(buyTx)) {
                modifiedDbBuys.add(buyTx);
            }

            TransactionsEntity sellTransaction = new TransactionsEntity();
            String transactionKey = "SELL_" + trade.getTradeId() + "_MATCH_" + buyTx.getTransactionId();
            sellTransaction.setTransactionId(UUID.nameUUIDFromBytes(transactionKey.getBytes()));
            sellTransaction.setTrade(sellTrade);
            sellTransaction.setBuyPrice(buyTx.getTrade().getPricePerStock());
            sellTransaction.setQuantity(matched);
            transactions.add(sellTransaction);

            TransactionProto proto = transactionMapper.toProto(sellTransaction);
            OutboxEventEntity event = new OutboxEventEntity();
            event.setTransactionOutboxId(UUID.nameUUIDFromBytes(proto.getTransactionId().getBytes()));
            event.setAggregateId(sellTransaction.getTransactionId());
            event.setPortfolioId(portfolioId);
            event.setPayload(proto.toByteArray());
            event.setStatus("PENDING");
            event.setCreatedAt(LocalDateTime.now());
            outboxEvents.add(event);

            remainingToMatch -= matched;
            if (remainingToMatch <= 0) break;
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