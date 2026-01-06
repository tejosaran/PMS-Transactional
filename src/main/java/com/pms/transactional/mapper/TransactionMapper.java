package com.pms.transactional.mapper;

import java.time.ZoneOffset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.protobuf.Timestamp;
import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.entities.TransactionsEntity;

@Component
public class TransactionMapper{

    @Autowired
    TradeMapper tradeMapper;
    
    public TransactionProto toProto(TransactionsEntity transaction){
        TradeProto trade = TradeProto.newBuilder()
                            .setTradeId(transaction.getTrade().getTradeId().toString())
                            .setPortfolioId(transaction.getTrade().getPortfolioId().toString())
                            .setSymbol(transaction.getTrade().getSymbol())
                            .setSide(transaction.getTrade().getSide().name())
                            .setPricePerStock(transaction.getTrade().getPricePerStock().doubleValue())
                            .setQuantity(transaction.getTrade().getQuantity())
                            .setTimestamp(Timestamp.newBuilder()
                                .setSeconds(transaction.getTrade().getTimestamp().toEpochSecond(ZoneOffset.UTC))
                                .setNanos(transaction.getTrade().getTimestamp().getNano())
                                .build())
                            .build();

        TransactionProto transactionProto = TransactionProto.newBuilder()
                                        .setTransactionId(transaction.getTransactionId().toString())
                                        .setPortfolioId(transaction.getTrade().getPortfolioId().toString())
                                        .setSymbol(transaction.getTrade().getSymbol())
                                        .setSide(transaction.getTrade().getSide().name())
                                        .setBuyPrice(transaction.getBuyPrice() == null ? "NA" :transaction.getBuyPrice().toPlainString())
                                        .setSellPrice(transaction.getTrade().getSide().name() == "SELL" ? transaction.getTrade().getPricePerStock().toString() : "NA")
                                        .setQuantity(transaction.getQuantity())
                                        .build();
        return transactionProto;
    }
}