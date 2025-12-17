package com.pms.transactional.dto;

import org.springframework.kafka.support.Acknowledgment;

import com.pms.transactional.TradeProto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TradeRecord {
    private TradeProto trade;
    private Acknowledgment ack; 
}