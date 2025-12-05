package com.pms.transactional.service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.dao.TransactionDao;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.entities.TransactionsEntity;
import com.pms.transactional.enums.TradeSide;

import jakarta.transaction.Transactional;

@Service
public class TransactionService {
    @Autowired
    private TransactionDao transactionDao;

    @Autowired
    private TradesDao tradesDao;

    @Transactional
    public void handleBuy(TradeProto trade) {

        UUID tradeId = UUID.fromString(trade.getTradeId());

        // Prevent duplicate trade_id insertion
        if (tradesDao.existsById(tradeId)) {
            throw new RuntimeException("Duplicate Trade ID: " + tradeId);
        }

        // INSERT into trades first
        TradesEntity te = new TradesEntity();
        te.setTradeId(tradeId);
        te.setPortfolioId(UUID.fromString(trade.getPortfolioId()));
        te.setSymbol(trade.getSymbol());
        te.setSide(TradeSide.BUY);
        te.setPricePerStock(BigDecimal.valueOf(trade.getPricePerStock()));
        te.setQuantity(trade.getQuantity());
        te.setTimestamp(LocalDateTime.now());

        tradesDao.save(te); // INSERT -- not UPDATE

        // Now create the transaction entry
        TransactionsEntity tx = new TransactionsEntity();
        tx.setTrade(te); // MANY-TO-ONE
        tx.setBuyPrice(BigDecimal.valueOf(trade.getPricePerStock()));
        tx.setSellPrice(null);
        tx.setSellQuantity(trade.getQuantity());
        tx.setRemainingQuantity(trade.getQuantity());

        transactionDao.save(tx);
    }

    @Transactional
    public void handleSell(TradeProto trade) {

        UUID tradeId = UUID.fromString(trade.getTradeId());
        if (tradesDao.existsById(tradeId)) {
            throw new RuntimeException("Duplicate Trade ID: " + tradeId);
        }
        long sellQty = trade.getQuantity();
        BigDecimal sellPrice = BigDecimal.valueOf(trade.getPricePerStock());
        UUID sellTradeId = UUID.fromString(trade.getTradeId());

        TradesEntity sellTrade = tradesDao.findById(sellTradeId)
                .orElseThrow(() -> new RuntimeException("Trade not found"));

        UUID portfolioId = sellTrade.getPortfolioId();
        String symbol = sellTrade.getSymbol();

        List<TransactionsEntity> buyList = transactionDao.findBuyOrdersFIFO(portfolioId, symbol);

        for (TransactionsEntity buyTx : buyList) {

            if (sellQty == 0)
                break;

            long available = buyTx.getRemainingQuantity();
            long matchedQty = Math.min(available, sellQty);

            // STEP 3: Reduce remaining quantity of BUY order
            buyTx.setRemainingQuantity(available - matchedQty);
            transactionDao.save(buyTx);

            // STEP 4: Create SELL transaction record
            TransactionsEntity sellTxn = new TransactionsEntity();

            // Hibernate will generate transactionId
            sellTxn.setTrade(sellTrade); // ManyToOne mapping
            sellTxn.setBuyPrice(buyTx.getBuyPrice()); // price at which stock was bought
            sellTxn.setSellPrice(sellPrice); // price at which selling now
            sellTxn.setSellQuantity(matchedQty); // how many filled
            sellTxn.setRemainingQuantity(0); // SELL has no remaining qty

            transactionDao.save(sellTxn);

            sellQty -= matchedQty;
        }

    }
}