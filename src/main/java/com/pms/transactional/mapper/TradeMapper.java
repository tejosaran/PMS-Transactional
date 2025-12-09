package com.pms.transactional.mapper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.pms.transactional.TradeProto;
import com.pms.transactional.entities.TradesEntity;
import com.pms.transactional.enums.TradeSide;

@Component
public class TradeMapper {

    public TradesEntity toEntity(TradeProto t) {

        TradesEntity e = new TradesEntity();

        e.setTradeId(UUID.fromString(t.getTradeId()));
        e.setPortfolioId(UUID.fromString(t.getPortfolioId()));
        e.setSymbol(t.getSymbol());
        e.setSide(TradeSide.valueOf(t.getSide()));
        e.setPricePerStock(new java.math.BigDecimal(t.getPricePerStock()));
        e.setQuantity(t.getQuantity());

        LocalDateTime ts = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(t.getTimestamp().getSeconds(), t.getTimestamp().getNanos()),
                ZoneOffset.UTC);
        e.setTimestamp(ts);

        return e;
    }

}
