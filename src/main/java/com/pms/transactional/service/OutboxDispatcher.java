package com.pms.transactional.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.dao.InvalidTradesDao;
import com.pms.transactional.dao.OutboxEventsDao;
import com.pms.transactional.entities.InvalidTradesEntity;
import com.pms.transactional.entities.OutboxEventEntity;

@Service
public class OutboxDispatcher implements SmartLifecycle {

    @Autowired
    private InvalidTradesDao invalidTradesDao;

    private OutboxEventsDao outboxDao;
    private OutboxEventProcessor processor;
    private AdaptiveBatchSizer batchSizer;

    private volatile boolean running = false;
    // private long backoffMs = 0;

    public OutboxDispatcher(
            OutboxEventsDao outboxDao,
            OutboxEventProcessor processor,
            AdaptiveBatchSizer batchSizer) {
        this.outboxDao = outboxDao;
        this.processor = processor;
        this.batchSizer = batchSizer;
    }

    @Override
    public void start() {
        running = true;
        Thread t = new Thread(this::loop, "outbox-dispatcher");
        t.setDaemon(true);
        t.start();
    }

    private void loop() {
        while (running) {
            try {
                ProcessingResult result = dispatchOnce();
                if (result.systemFailure()) {
                    Thread.sleep(1000); // wait ONLY on system failure
                }
                // Thread.sleep(50);
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    @Transactional
    protected ProcessingResult dispatchOnce() {

        int limit = batchSizer.getCurrentSize();

        // List<OutboxEventEntity> batch =
        // outboxDao.findByStatusOrderByCreatedAt(
        // "PENDING",
        // PageRequest.of(0, limit)
        // );

        List<OutboxEventEntity> batch = outboxDao.findPendingWithPortfolioXactLock(limit);
        if (batch.isEmpty()) {
            batchSizer.reset();
            return ProcessingResult.success(List.of());
        }

        ProcessingResult result = processor.process(batch);

        if (!result.successfulIds().isEmpty()) {
            outboxDao.markAsSent(result.successfulIds());
        }

        if (result.poisonPill() != null) {

            OutboxEventEntity poison = result.poisonPill();

            // 1️⃣ Save into invalid_trades
            InvalidTradesEntity invalid = new InvalidTradesEntity();
            invalid.setAggregateId(poison.getAggregateId());
            invalid.setPayload(poison.getPayload());
            invalid.setErrorMessage("Poison pill – processing failed");

            invalidTradesDao.save(invalid);
            // outboxDao.delete(result.poisonPill());
            outboxDao.markAsFailed(poison.getTransactionOutboxId());
        }
        return result;

    }
    // @Override
    // public void start() {
    // running = true;
    // new Thread(this::dispatchLoop, "outbox-dispatcher").start();
    // } 3/1/2026

    // private void dispatchLoop() {
    // while (running) {
    // try {
    // if (backoffMs > 0) {
    // Thread.sleep(backoffMs);
    // }

    // long start = System.currentTimeMillis();
    // int limit = batchSizer.getCurrentSize();

    // List<OutboxEventEntity> batch =
    // outboxDao.findByStatusOrderByCreatedAt(
    // "PENDING",
    // PageRequest.of(0, limit)
    // );

    // if (batch.isEmpty()) {
    // batchSizer.reset();
    // backoffMs = 0;
    // Thread.sleep(50);
    // continue;
    // }

    // ProcessingResult result = processor.process(batch);

    // // PREFIX-SAFE UPDATE
    // if (!result.successfulIds().isEmpty()) {
    // outboxDao.markAsSent(result.successfulIds());
    // }

    // // POISON PILL
    // if (result.poisonPill() != null) {
    // // TODO: DLQ
    // outboxDao.delete(result.poisonPill());
    // }

    // // SYSTEM FAILURE
    // if (result.systemFailure()) {
    // backoffMs = backoffMs == 0 ? 1000 : Math.min(backoffMs * 2, 30000);
    // continue;
    // }

    // long duration = System.currentTimeMillis() - start;
    // batchSizer.adjust(duration, batch.size());
    // backoffMs = 0;

    // } catch (Exception e) {
    // backoffMs = 1000;
    // }
    // }
    // } 3/1/2026

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
