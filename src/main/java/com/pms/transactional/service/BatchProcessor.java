package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.UUID;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.dto.TradeRecord;
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
    private BlockingQueue<TradeRecord>  buffer;
    @Autowired
    private TradesDao tradesDao;
    @Autowired
    private TransactionDao transactionDao;
    @Autowired
    private OutboxEventsDao outboxDao;
    @Autowired
    private TransactionService transactionService;
    private static final int BATCH_SIZE = 10;
    // private static final long FLUSH_INTERVAL_MS = 10000;
    // private long lastFlushTime = System.currentTimeMillis();

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
        List<TradeRecord> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);

        if (batch.isEmpty()){
            return;
        }

        List<TradeRecord> buyBatch = batch.stream()
                                            .filter(record->(record.getTrade().getSide()).equals("BUY"))
                                            .collect(Collectors.toList());

        List<TradeRecord> sellBatch = batch.stream()
                                            .filter(record->(record.getTrade().getSide()).equals("SELL"))
                                            .collect(Collectors.toList());

        processBuyBatch(buyBatch);
        processSellBatch(sellBatch);

        // List<TradesEntity> trades = new ArrayList<>();
        // List<TransactionsEntity> txns = new ArrayList<>();
        // List<OutboxEventEntity> outbox = new ArrayList<>();

        // for (TradeRecord record : buyBatch){
        //     TradeProto proto = record.getTrade();
        //     try{
        //         transactionService.processBuy(proto, trades, txns, outbox);
        //     }
        //     catch(InvalidTradeException ex){
        //         //add to invalidtrades table 
        //     }
            
        // }

        // tradesDao.saveAll(trades);
        // transactionDao.saveAll(txns);
        // outboxDao.saveAll(outbox);

        // batch.forEach(r -> r.getAck().acknowledge());

        // System.out.println("Buy Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }

    @Transactional
    public void processBuyBatch(List<TradeRecord> buyBatch){
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();

        for (TradeRecord record : buyBatch){
            TradeProto proto = record.getTrade();
            transactionService.processBuy(proto, trades, txns, outbox);
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        outboxDao.saveAll(outbox);

        buyBatch.forEach(r -> r.getAck().acknowledge());

        System.out.println("Buy Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }

    @Transactional
    public void processSellBatch(List<TradeRecord> sellBatch){
        List<TradesEntity> trades = new ArrayList<>();
        List<TransactionsEntity> txns = new ArrayList<>();
        List<OutboxEventEntity> outbox = new ArrayList<>();
        List<TransactionsEntity> updatedBuys = new ArrayList<>();

        Set<UUID> portfolioIds = sellBatch.stream()
                                            .map(r -> UUID.fromString(r.getTrade().getPortfolioId()))
                                            .collect(Collectors.toSet()); 

        Set<String> symbols = sellBatch.stream()
                                        .map(record->record.getTrade().getSymbol())
                                        .collect(Collectors.toSet());

        List<TransactionsEntity> eligibleBuys = transactionDao.findEligibleBuys(new ArrayList<>(portfolioIds),new ArrayList<>(symbols),TradeSide.BUY);

        Map<String, List<TransactionsEntity>> buyMap = eligibleBuys.stream() .collect(Collectors.groupingBy( b -> b.getTrade().getPortfolioId() + "_" + b.getTrade().getSymbol(), LinkedHashMap::new, Collectors.toList() ));

        for (TradeRecord record : sellBatch){
            TradeProto proto = record.getTrade();
            try{
                transactionService.processSell(proto,buyMap, updatedBuys,trades, txns, outbox);
            }
            catch(InvalidTradeException ex){
                logger.info("Invalid trade message detected");
                //add to invalidtrades table 
            }
        }

        tradesDao.saveAll(trades);
        transactionDao.saveAll(txns);
        transactionDao.saveAll(updatedBuys);
        outboxDao.saveAll(outbox);

        sellBatch.forEach(r -> r.getAck().acknowledge());

        System.out.println("Sell Batch Flushed: Trades=" + trades.size() + " Transactions=" + txns.size() + " Outbox=" + outbox.size());
    }
}