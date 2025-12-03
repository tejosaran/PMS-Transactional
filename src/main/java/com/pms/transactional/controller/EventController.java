package com.pms.transactional.controller;

import java.time.ZoneOffset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.pms.transactional.TradeProto.newBuilder;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TradeSideProto;
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dto.TradeDTO;
import com.pms.transactional.dto.TransactionDTO;
import com.pms.transactional.service.KafkaMessagePublisher;
import com.pms.transactional.service.KafkaTradeMessagePublisher;

@RequestMapping
@RestController

public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @Autowired
    private KafkaTradeMessagePublisher tradePublisher;

    // @GetMapping("/publish/{message}")
    // public ResponseEntity<?> publishMessage(@PathVariable String message) {
    // try {

    // publisher.publishMessage(message);

    // return ResponseEntity.ok("Message published successfully");
    // } catch (Exception e) {
    // return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    // }

    // }

    @PostMapping("/transactions/publish")
    public ResponseEntity<?> publishMessage(@RequestBody TransactionDTO transaction) {
        System.out.println("Hello");
        try {
            TransactionProto txn = convertDTOToProto(transaction);
            publisher.publishMessage(transaction.getTrade().getPortfolioId().toString(), txn);
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to publish message: " + e.getMessage());
        }
    }

    @PostMapping("/trades/publish")
    public ResponseEntity<?> publishTradeMessage(@RequestBody TradeDTO trade) {
        System.out.println("Hello");
        try {
            TradeProto tradeProto = convertDTOToProto(trade);
            tradePublisher.publishTradeMessage(
                    trade.getPortfolioId().toString(),
                    tradeProto);

            return ResponseEntity.ok("Trade Message published successfully");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to publish trade message: " + e.getMessage());
        }
    }

    private TransactionProto convertDTOToProto(TransactionDTO transaction) {
        return TransactionProto.newBuilder()
                .setTransactionId(transaction.getTransactionId().toString()) // UUID as String
                .setBuyPrice(transaction.getBuyPrice().toString()) // BigDecimal as double
                .setSellPrice(transaction.getSellPrice().toString()) // BigDecimal as double
                .setRemainingQuantity(transaction.getRemainingQuantity())
                .setTrade(newBuilder()
                        .setTradeId(transaction.getTrade().getTradeId().toString())
                        .setPortfolioId(transaction.getTrade().getPortfolioId().toString())
                        .setSymbol(transaction.getTrade().getSymbol())
                        .setSide(com.pms.transactional.TradeSideProto.valueOf(transaction.getTrade().getSide().name()))
                        .setPricePerStock(transaction.getTrade().getPricePerStock().doubleValue())
                        .setQuantity(transaction.getTrade().getQuantity())
                        .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(transaction.getTrade().getTimestamp().toEpochSecond(ZoneOffset.UTC))
                                .setNanos(transaction.getTrade().getTimestamp().getNano())
                                .build())
                        .build())
                .build();
    }

    private TradeProto convertDTOToProto(TradeDTO trade) {
        return TradeProto.newBuilder()
                .setTradeId(trade.getTradeId().toString())
                .setPortfolioId(trade.getPortfolioId().toString())
                .setSymbol(trade.getSymbol())
                .setSide(TradeSideProto.valueOf(trade.getSide().name()))
                .setPricePerStock(trade.getPricePerStock().doubleValue())
                .setQuantity(trade.getQuantity())
                .setTimestamp(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(trade.getTimestamp().toEpochSecond(java.time.ZoneOffset.UTC))
                                .setNanos(trade.getTimestamp().getNano())
                                .build())
                .build();
    }

}
