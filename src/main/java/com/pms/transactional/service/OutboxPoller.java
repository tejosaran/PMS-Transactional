// package com.pms.transactional.service;

// import java.util.List;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.scheduling.annotation.EnableScheduling;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Service;

// import com.pms.transactional.TransactionProto;
// import com.pms.transactional.dao.OutboxEventsDao;
// import com.pms.transactional.entities.OutboxEventEntity;

// @Service
// @EnableScheduling
// public class OutboxPoller {

//     @Autowired
//     OutboxEventsDao outboxDao;

//     @Autowired
//     private KafkaTemplate<String, Object> kafkaTemplate;

//     @Scheduled(fixedRate = 10000)
//     public void pollAndPublish() {
//         List<OutboxEventEntity> pendingList = outboxDao.findByStatusOrderByCreatedAt("PENDING");

//         for (OutboxEventEntity event : pendingList) {
//             try{
//                 TransactionProto proto = TransactionProto.parseFrom(event.getPayload());
//                 kafkaTemplate.send("transactions-topic", proto).get();
//                 event.setStatus("SENT");
//                 outboxDao.save(event);

//             } catch(Exception e) {
//                 e.printStackTrace();
//                 event.setStatus("FAILED");
//                 outboxDao.save(event);
//             }
//         }
        

//     }

// }

package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.TransactionProto;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.OutboxEventEntity;

@Service
@EnableScheduling
public class OutboxPoller {

    private int BATCH_SIZE = 20;
    private String STATUS_PENDING = "PENDING";
    private String STATUS_IN_PROGRESS = "IN_PROGRESS";
    private String STATUS_SENT = "SENT";

    @Autowired
    private OutboxEventsDao outboxDao;

    @Autowired
    @Qualifier("kafkaOutboxTemplate")
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedRate = 20000)
    @Transactional
    public void pollAndPublishBatch() {

        List<OutboxEventEntity> batch =
                outboxDao.findByStatusOrderByCreatedAt(
                        STATUS_PENDING,
                        PageRequest.of(0, BATCH_SIZE)
                );

        if (batch.isEmpty()) {
            return;
        }

        try {
            batch.forEach(e -> e.setStatus(STATUS_IN_PROGRESS));
            outboxDao.saveAll(batch);

            List<TransactionProto> protoBatch = new ArrayList<>();
            for (OutboxEventEntity event : batch) {
                protoBatch.add(TransactionProto.parseFrom(event.getPayload()));
            }

            kafkaTemplate.executeInTransaction(kt -> {
                protoBatch.forEach(proto ->
                        kt.send("transactions-topic", proto)
                );
                return true;
            });

            batch.forEach(e -> e.setStatus(STATUS_SENT));
            outboxDao.saveAll(batch);

        } catch (Exception ex) {
            batch.forEach(e -> e.setStatus(STATUS_PENDING));
            outboxDao.saveAll(batch);

            ex.printStackTrace();
        }
    }
}
