package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.UUID;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
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

import jakarta.transaction.Transactional;

@Service
public class BatchProcessor{
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);
    @Autowired
    private BlockingQueue<TradeProto>  buffer;
    @Autowired
    private TradesDao tradesDao;
    @Autowired
    private TransactionDao transactionDao;
    @Autowired
    private OutboxEventsDao outboxDao;

    @Autowired
    private InvalidTradesDao invalidTradesDao;
    @Autowired
    private TransactionService transactionService;
    private static final int BATCH_SIZE = 10;

    public void checkAndFlush() {
        // long now = System.currentTimeMillis();
        boolean sizeExceeded = buffer.size() >= BATCH_SIZE;
        // boolean timeExceeded = (now - lastFlushTime) >= FLUSH_INTERVAL_MS;

        if(sizeExceeded){
            flushBatch();
            // lastFlushTime = now;
        }
    }

    public void flushBatch() {
        List<TradeProto> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty()){
            return;
        }

        List<TradeProto> buyBatch = batch.stream()
                                            .filter(record->(record.getSide()).equals("BUY"))
                                            .collect(Collectors.toList());

        List<TradeProto> sellBatch = batch.stream()
                                            .filter(record->(record.getSide()).equals("SELL"))
                                            .collect(Collectors.toList());

        processBuyBatch(buyBatch);
        processSellBatch(sellBatch);

    }

    @Transactional
    public void processBuyBatch(List<TradeProto> buyBatch){
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();

        for (TradeProto record : buyBatch){
            transactionService.processBuy(record, trades, txns, outbox);
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        outboxDao.saveAll(outbox);

        System.out.println("Buy Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }

    @Transactional
    public void processSellBatch(List<TradeProto> sellBatch){
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();
        List<TransactionsEntity> updatedBuys = new ArrayList<>();
        List<InvalidTradesEntity> invalidTrades = new ArrayList<>();

        Set<UUID> portfolioIds = sellBatch.stream()
                                            .map(record -> UUID.fromString(record.getPortfolioId()))
                                            .collect(Collectors.toSet()); 

        Set<String> symbols = sellBatch.stream()
                                        .map(record->record.getSymbol())
                                        .collect(Collectors.toSet());

        List<TransactionsEntity> eligibleBuys = transactionDao.findEligibleBuys(new ArrayList<>(portfolioIds),new ArrayList<>(symbols),TradeSide.BUY);

        Map<String, List<TransactionsEntity>> buyMap = eligibleBuys.stream().collect(Collectors.groupingBy( b -> b.getTrade().getPortfolioId() + "_" + b.getTrade().getSymbol(), LinkedHashMap::new, Collectors.toList() ));

        for (TradeProto record : sellBatch){
            try{
                transactionService.processSell(record,buyMap, updatedBuys,trades, txns, outbox);
            }
            catch(InvalidTradeException ex){
                logger.info("Invalid trade message detected");
                transactionService.handleInvalid(record, invalidTrades, ex.getErrorMessage());
            }
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        transactionDao.saveAll(updatedBuys);
        outboxDao.saveAll(outbox);

        if(invalidTrades.size()>0){
            invalidTradesDao.saveAll(invalidTrades);
        }
        System.out.println("Sell Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }
}