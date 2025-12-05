package com.pms.transactional.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TradeSideProto;
import com.pms.transactional.dao.TradesDao;
import com.pms.transactional.mapper.TradeMapper;

@Service
public class KafkaTradeMessageListner {

    Logger logger = LoggerFactory.getLogger(KafkaMessageListner.class);

    @Autowired
    private TradesDao tradesDao;

    @Autowired
    private TradeMapper mapper;

    @Autowired
    private TransactionService transactionService;

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000, multiplier = 2, maxDelay = 10000))
    @KafkaListener(topics = "validatedtrades-topic", groupId = "trades", containerFactory = "tradekafkaListenerContainerFactory")
    public void consume2(TradeProto trade) {
            logger.info("Consumer message (parsed): {}", trade);
            if (trade.getSide() == TradeSideProto.BUY) {
                System.out.println("It is a buy trade");
                transactionService.handleBuy(trade);
            } else if (trade.getSide() == TradeSideProto.SELL) {
                System.out.println("It is a sell trade");
                transactionService.handleSell(trade);
            }

    }

    @DltHandler
    public void ListenDLT(TradeProto trade) {
        logger.error("DLT reached for trade: {}", trade);
    }

}
