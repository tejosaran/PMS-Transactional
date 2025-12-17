package com.pms.transactional.config;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pms.transactional.dto.TradeRecord;


@Configuration
public class BufferConfig{
    @Bean
    public BlockingQueue<TradeRecord> eventBuffer() {
        return new LinkedBlockingQueue<>(50000);  
    }
}