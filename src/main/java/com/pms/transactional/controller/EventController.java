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
import com.pms.transactional.TransactionProto;
import com.pms.transactional.dto.TransactionDTO;
import com.pms.transactional.service.KafkaMessagePublisher;

@RestController
@RequestMapping("/producer")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    // @GetMapping("/publish/{message}")
    // public ResponseEntity<?> publishMessage(@PathVariable String message) {
    // try {

    // publisher.publishMessage(message);

    // return ResponseEntity.ok("Message published successfully");
    // } catch (Exception e) {
    // return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    // }

    // }

    @PostMapping("/publish")
    public ResponseEntity<?> publishMessage(@RequestBody TransactionDTO transaction) {
        try {
            TransactionProto txn = convertJsonToProto(transaction);
            publisher.publishMessage(transaction.getTrade().getPortfolioId().toString(),txn);
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to publish message: " + e.getMessage());
        }
    }

    private TransactionProto convertJsonToProto(TransactionDTO transaction) {
        return TransactionProto.newBuilder()
                .setTransactionId(transaction.getTransactionId().toString())  // UUID as String
                .setBuyPrice(transaction.getBuyPrice().toString())  // BigDecimal as double
                .setSellPrice(transaction.getSellPrice().toString())  // BigDecimal as double
                .setRemainingQuantity(transaction.getRemainingQuantity())
                .setTrade(newBuilder()
                        .setTradeId(transaction.getTrade().getTradeId().toString())
                        .setPortfolioId(transaction.getTrade().getPortfolioId().toString())
                        .setSymbol(transaction.getTrade().getSymbol())
                        .setSide(com.pms.transactional.TradeSideProto.valueOf(transaction.getTrade().getSide().name()))
                        .setPricePerStock(transaction.getTrade().getPricePerStock().doubleValue())
                        .setQuantity(transaction.getTrade().getQuantity())
                        .setTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(transaction.getTrade().getTimestamp().toEpochSecond(ZoneOffset.UTC))
                                .setNanos(transaction.getTrade().getTimestamp().getNano())
                                .build())
                        .build())
                .build();
    }

}
