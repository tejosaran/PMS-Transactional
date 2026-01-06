package com.pms.transactional.controller;

import java.time.ZoneOffset;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dto.TradeDTO;
import com.pms.transactional.service.KafkaTradeMessagePublisher;

@RequestMapping
@RestController
public class EventControllerMulti{

    @Autowired
    private KafkaTradeMessagePublisher tradePublisher;

    @PostMapping("/trades/publish/multi")
    public ResponseEntity<?> publishTradeMessages(@RequestBody List<TradeDTO> trades) {
        System.out.println("Publishing " + trades.size() + " trade messages");
        try {
            for (TradeDTO trade : trades) {
                TradeProto tradeProto = convertDTOToProto(trade);

                tradePublisher.publishTradeMessage(
                        trade.getPortfolioId().toString(),
                        tradeProto
                );
            }

            return ResponseEntity.ok("Trade messages published successfully");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to publish trade messages: " + e.getMessage());
        }
    }

    private TradeProto convertDTOToProto(TradeDTO trade) {
        return TradeProto.newBuilder()
                .setTradeId(trade.getTradeId().toString())
                .setPortfolioId(trade.getPortfolioId().toString())
                .setSymbol(trade.getSymbol())
                .setSide(trade.getSide().name())
                .setPricePerStock(trade.getPricePerStock().doubleValue())
                .setQuantity(trade.getQuantity())
                .setTimestamp(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(trade.getTimestamp().toEpochSecond(ZoneOffset.UTC))
                                .setNanos(trade.getTimestamp().getNano())
                                .build()
                )
                .build();
    }
}

