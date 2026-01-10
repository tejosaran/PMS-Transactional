package com.pms.transactional.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import com.pms.transactional.TradeProto;


@Service
public class BatchProcessor implements SmartLifecycle{
    Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private BlockingQueue<TradeProto> buffer;
   
    @Autowired
    private TransactionService transactionService;

    private static final int BATCH_SIZE = 5000;
    private static final long FLUSH_INTERVAL_MS = 10000;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private boolean isRunning = false;

    public void checkAndFlush(){
        if(buffer.size() >= BATCH_SIZE){
            flushBatch();
        }
    }

    public synchronized void flushBatch(){
        if(buffer.isEmpty()) return;

        List<TradeProto> batch = new ArrayList<>(BATCH_SIZE);
        buffer.drainTo(batch, BATCH_SIZE);

        try{
            Map<String, List<TradeProto>> grouped = batch.stream().collect(Collectors.groupingBy(TradeProto::getSide));
            transactionService.processUnifiedBatch(grouped.getOrDefault("BUY", List.of()), grouped.getOrDefault("SELL", List.of()));
        }
        catch(DataAccessResourceFailureException e) {
            logger.error("DB Connection failure. Pausing consumer.");
            handleDatabaseDown();
            throw e;
        }
        catch(DataIntegrityViolationException e){
            String rootMsg = (e.getRootCause() != null) ? e.getRootCause().getMessage() : e.getMessage();
            logger.error("DATA ERROR: Database rejected the batch. Reason: {}", rootMsg);
            throw e;
        }   
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void start() {
        logger.info("BatchProcessor starting: Initializing time-based flush heartbeat");
        scheduler.scheduleWithFixedDelay(this::flushBatch, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.isRunning = true;
    }

    @Override
    public void stop(Runnable callback) {
        logger.info("BatchProcessor stopping: Performing final flush");
        scheduler.shutdown();

        if(!buffer.isEmpty()){
            flushBatch();
        }
        this.isRunning = false;
        callback.run();
    }

    @Override
    public void stop(){};

    @Override
    public int getPhase(){
        return Integer.MAX_VALUE;
    }

    private boolean isRecovering = false;
    private ScheduledFuture<?> recoveryTask;
    private static final String CONSUMER_ID = "tradesConsumer";

    private void handleDatabaseDown(){
        synchronized(this){
            if(isRecovering){
                return;
            }
            isRecovering=true;
        }

        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(CONSUMER_ID);
        if(container != null){
            container.pause();
            logger.warn("Kafka Consumer paused. Starting background probe daemon...");
        }
        startDaemon();
    }
        
    private void startDaemon() {
        recoveryTask = scheduler.scheduleWithFixedDelay(() -> {
            try{
                jdbcTemplate.execute("SELECT 1");
                logger.info("Database is up! Resuming consumer and stopping daemon.");

                MessageListenerContainer container = kafkaListenerEndpointRegistry
                        .getListenerContainer(CONSUMER_ID);
                if(container != null) container.resume();

                synchronized(this){
                    isRecovering = false;
                    if (recoveryTask != null) {
                        recoveryTask.cancel(false);
                        recoveryTask = null;
                    }
                }
            } 
            catch(Exception e) {
                logger.warn("Daemon: Database still down. Retrying in 10s...");
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
}