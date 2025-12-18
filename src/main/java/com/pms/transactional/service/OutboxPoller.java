package com.pms.transactional.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.OutboxEventEntity;

@Service
@EnableScheduling
public class OutboxPoller {

    @Autowired
    OutboxEventsDao outboxDao;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedRate = 600000)
    public void pollAndPublish() {
        List<OutboxEventEntity> pendingList = outboxDao.findByStatusOrderByCreatedAt("PENDING");

        for (OutboxEventEntity event : pendingList) {
            try{
                TransactionProto proto = TransactionProto.parseFrom(event.getPayload());
                kafkaTemplate.send("transactions-topic", proto).get();
                event.setStatus("SENT");
                outboxDao.save(event);

            } catch(Exception e) {
                e.printStackTrace();
                event.setStatus("FAILED");
                outboxDao.save(event);
            }
        }
        

    }

}