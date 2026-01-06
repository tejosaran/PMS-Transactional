package com.pms.transactional.config;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pms.transactional.TradeProto;
import com.pms.transactional.dto.TradeRecord;


@Configuration
public class BufferConfig{
    @Bean
public BlockingQueue<TradeProto> protoBuffer() {
    return new LinkedBlockingQueue<>(20000);
}
    @Bean
public BlockingQueue<List<TradeProto>> protoListBuffer() {
    return new LinkedBlockingQueue<>(20000);
}


@Bean
public BlockingQueue<TradeRecord> recordBuffer() {
    return new LinkedBlockingQueue<>(50000);
}

}