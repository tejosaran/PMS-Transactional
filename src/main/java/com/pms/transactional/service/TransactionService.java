package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
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


@Service
public class TransactionService{

    @Autowired
    private TransactionDao transactionDao;

    @Autowired
    private TradesDao tradesDao;

    @Autowired
    private OutboxEventsDao outboxDao;

    @Autowired
    private TransactionMapper transactionMapper;

    Logger logger = LoggerFactory.getLogger(TransactionService.class);

    public void processBuy(TradeProto trade,List<TradesEntity> trades,List<TransactionsEntity> txns,List<OutboxEventEntity> outbox) {

        UUID tradeId = UUID.fromString(trade.getTradeId());

        if (tradesDao.existsById(tradeId)) {
            logger.error("Trade with ID {} already exists. Rejecting duplicate trade.", tradeId);
            return;
        }

        TradesEntity buyTrade = new TradesEntity();
        buyTrade.setTradeId(tradeId);
        buyTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        buyTrade.setSymbol(trade.getSymbol());
        buyTrade.setSide(TradeSide.BUY);
        buyTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        buyTrade.setQuantity(trade.getQuantity());
        buyTrade.setTimestamp(LocalDateTime.ofInstant(Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),ZoneOffset.UTC));
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
            event.setStatus("PENDING");
            event.setCreatedAt(LocalDateTime.now());
            outbox.add(event);
        }
    }

     public void processSell(TradeProto trade,Map<String, List<TransactionsEntity>> allBuys, List<TransactionsEntity> updatedBuys,List<TradesEntity> trades,List<TransactionsEntity> txns,List<OutboxEventEntity> outbox) {
        UUID tradeId = UUID.fromString(trade.getTradeId());

        if(tradesDao.existsById(tradeId)){
            logger.error("Trade with ID {} already exists. Rejecting duplicate trade.", tradeId);
            return;
        }

        TradesEntity sellTrade = new TradesEntity();
        sellTrade.setTradeId(tradeId);
        sellTrade.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        sellTrade.setSymbol(trade.getSymbol());
        sellTrade.setSide(TradeSide.SELL);
        sellTrade.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        sellTrade.setQuantity(trade.getQuantity());
        sellTrade.setTimestamp(LocalDateTime.ofInstant(Instant.ofEpochSecond(trade.getTimestamp().getSeconds(), trade.getTimestamp().getNanos()),ZoneOffset.UTC));

        trades.add(sellTrade);

        long qtyToSell = trade.getQuantity(); 
        List<TransactionsEntity> allEligibleBuys = allBuys.get( sellTrade.getPortfolioId() + "_" + sellTrade.getSymbol()); 
        if (allEligibleBuys == null) { throw new InvalidTradeException("No eligible buys for SELL " + tradeId); }

        List<TransactionsEntity> eligibleBuys = allEligibleBuys.stream()
                                                                .filter(buy -> buy.getTrade().getTimestamp().isBefore(sellTrade.getTimestamp()))
                                                                .collect(Collectors.toList());

        long totalAvailable = eligibleBuys.stream()
                                     .mapToLong(TransactionsEntity::getQuantity)
                                     .sum();

        if(totalAvailable < qtyToSell){
            throw new InvalidTradeException(
                    "Insufficient quantity. Available=" + totalAvailable +
                    ", Required=" + qtyToSell +
                    ", TradeId=" + trade.getTradeId()
            );
        }

        for(TransactionsEntity buyTx : eligibleBuys){

            if(qtyToSell <= 0) break;
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

            if(!outboxDao.existsByAggregateId(sellTxn.getTransactionId())){
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

    public void handleInvalid(TradeProto trade,List<InvalidTradesEntity> invalidTrades, String errorMessage){
            InvalidTradesEntity invalidTrade = new InvalidTradesEntity();
            invalidTrade.setAggregateId(UUID.fromString(trade.getTradeId()));
            invalidTrade.setPayload(trade.toByteArray());
            invalidTrade.setErrorMessage(errorMessage);

            invalidTrades.add(invalidTrade);
    }

}