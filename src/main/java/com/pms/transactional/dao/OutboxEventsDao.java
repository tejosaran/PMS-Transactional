package com.pms.transactional.dao;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.entities.OutboxEventEntity;

@Repository
public interface OutboxEventsDao extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAt(
        String status,
        Pageable pageable
    );
    boolean existsByAggregateId(UUID aggregateId);

    @Modifying
    @Transactional
    @Query("update OutboxEventEntity e set e.status = 'SENT' where e.transactionOutboxId in :ids")
    void markAsSent(List<UUID> ids);
}
