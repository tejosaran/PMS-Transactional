package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.InvalidTradesDao;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.dao.TradesDao;
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
    private TradesDao tradesDao;

    @Autowired
    private OutboxEventsDao outboxDao;

    @Autowired
    private InvalidTradesDao invalidTradesDao;

    @Autowired
    private TransactionMapper transactionMapper;

    Logger logger = LoggerFactory.getLogger(TransactionService.class);

    @Transactional
    public void processUnifiedBatch(List<TradeProto> buyBatch, List<TradeProto> sellBatch) {
        try {
            if (!buyBatch.isEmpty())
                processBuyBatch(buyBatch);
            if (!sellBatch.isEmpty())
                processSellBatch(sellBatch);
        } catch (DataIntegrityViolationException e) {
            logger.error("Conflict detected in Database. Rolling back batch to prevent duplicates.");
            throw e;
        }
    }

    @Transactional
    public void processBuyBatch(List<TradeProto> buyBatch) {
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();

        for (TradeProto record : buyBatch) {
            UUID tradeId = UUID.fromString(record.getTradeId());
            if (tradesDao.existsById(tradeId)) {
                logger.info("Skipping BUY trade {} - already processed", tradeId);
                continue;
            }
            processBuy(record, trades, txns, outbox);
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        outboxDao.saveAll(outbox);

        System.out.println("Buy Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox="
                + outbox.size());
    }

    @Transactional
    public void processSellBatch(List<TradeProto> sellBatch) {
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();
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

        for (TradeProto record : sellBatch) {
            UUID tradeId = UUID.fromString(record.getTradeId());

            if (tradesDao.existsById(tradeId)) {
                logger.info("Skipping SELL trade {} - already processed", tradeId);
                continue;
            }
            try {
                processSell(record, buyMap, updatedBuys, trades, txns, outbox);
            } catch (InvalidTradeException ex) {
                logger.info("Invalid trade message detected");
                handleInvalid(record, invalidTrades, ex.getErrorMessage());
            }
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        transactionDao.saveAll(new ArrayList<>(new LinkedHashSet<>(updatedBuys)));
        outboxDao.saveAll(outbox);

        if (!invalidTrades.isEmpty()) {
            invalidTradesDao.saveAll(invalidTrades);
        }
        System.out.println("Sell Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox="
                + outbox.size());
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
        buyTxn.setTrade(buyTrade);
        buyTxn.setBuyPrice(null);
        buyTxn.setQuantity(trade.getQuantity());
        txns.add(buyTxn);

        if (!outboxDao.existsByAggregateId(buyTxn.getTransactionId())) {
            TransactionProto proto = transactionMapper.toProto(buyTxn);
            OutboxEventEntity event = new OutboxEventEntity();
            event.setAggregateId(buyTxn.getTransactionId());
            event.setPayload(proto.toByteArray());
            event.setPortfolioId(UUID.fromString(proto.getPortfolioId()));
            event.setStatus("PENDING");

            event.setCreatedAt(LocalDateTime.now());
            outbox.add(event);
        }
    }

    public void processSell(TradeProto trade, Map<String, List<TransactionsEntity>> allBuys,
            List<TransactionsEntity> updatedBuys, List<TradesEntity> trades, List<TransactionsEntity> txns,
            List<OutboxEventEntity> outbox) {
        UUID tradeId = UUID.fromString(trade.getTradeId());

        TradesEntity sellTrade = new TradesEntity();
        sellTrade.setTradeId(tradeId);
        sellTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        sellTrade.setSymbol(trade.getSymbol());
        sellTrade.setSide(TradeSide.SELL);
        sellTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        sellTrade.setQuantity(trade.getQuantity());
        sellTrade.setTimestamp(LocalDateTime.ofInstant(
                Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),
                ZoneOffset.UTC));

        trades.add(sellTrade);

        long qtyToSell = trade.getQuantity();
        List<TransactionsEntity> allEligibleBuys = allBuys
                .get(sellTrade.getPortfolioId() + "_" + sellTrade.getSymbol());
        if (allEligibleBuys == null) {
            throw new InvalidTradeException("No eligible buys for SELL " + tradeId);
        }

        List<TransactionsEntity> eligibleBuys = allEligibleBuys.stream()
                .filter(buy -> buy.getTrade().getTimestamp().isBefore(sellTrade.getTimestamp()))
                .filter(buy -> buy.getQuantity() > 0)
                .collect(Collectors.toList());

        long totalAvailable = eligibleBuys.stream()
                .mapToLong(TransactionsEntity::getQuantity)
                .sum();

        if (totalAvailable < qtyToSell) {
            throw new InvalidTradeException(
                    "Insufficient quantity. Available=" + totalAvailable +
                            ", Required=" + qtyToSell +
                            ", TradeId=" + trade.getTradeId());
        }

        for (TransactionsEntity buyTx : eligibleBuys) {

            if (qtyToSell <= 0)
                break;
            long available = buyTx.getQuantity();
            long matchedQty = Math.min(available, qtyToSell);

            buyTx.setQuantity(available - matchedQty);
            updatedBuys.add(buyTx);

            TransactionsEntity sellTxn = new TransactionsEntity();
            sellTxn.setTrade(sellTrade);
            sellTxn.setBuyPrice(buyTx.getTrade().getPricePerStock());
            sellTxn.setQuantity(matchedQty);
            txns.add(sellTxn);

            qtyToSell -= matchedQty;

            if (!outboxDao.existsByAggregateId(sellTxn.getTransactionId())) {
                TransactionProto proto = transactionMapper.toProto(sellTxn);
                OutboxEventEntity event = new OutboxEventEntity();
                event.setAggregateId(sellTxn.getTransactionId());
                event.setPayload(proto.toByteArray());
                event.setStatus("PENDING");
                event.setCreatedAt(LocalDateTime.now());
                outbox.add(event);
            }
        }
        System.out.println();
    }

    public void handleInvalid(TradeProto trade, List<InvalidTradesEntity> invalidTrades, String errorMessage) {
        InvalidTradesEntity invalidTrade = new InvalidTradesEntity();
        invalidTrade.setAggregateId(UUID.fromString(trade.getTradeId()));
        invalidTrade.setPayload(trade.toByteArray());
        invalidTrade.setErrorMessage(errorMessage);

        invalidTrades.add(invalidTrade);
    }

}