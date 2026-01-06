package com.pms.transactional.dao;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.pms.transactional.entities.OutboxEventEntity;

@Repository
public interface OutboxEventsDao extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAt(
            String status,
            Pageable pageable);

    boolean existsByAggregateId(UUID aggregateId);

    @Query(value = """
            SELECT DISTINCT ON (portfolio_id) *
            FROM outbox_events
            WHERE status = 'PENDING'
              AND pg_try_advisory_xact_lock(
                    hashtext(portfolio_id::text)
              )
            ORDER BY portfolio_id, created_at
            LIMIT :limit
            """, nativeQuery = true)
    List<OutboxEventEntity> findPendingWithPortfolioXactLock(
            @Param("limit") int limit);

    @Modifying
    @Transactional
    @Query("update OutboxEventEntity e set e.status = 'SENT' where e.transactionOutboxId in :ids")
    void markAsSent(List<UUID> ids);

    @Modifying
    @Query("""
                update OutboxEventEntity e
                set e.status = 'FAILED'
                where e.transactionOutboxId = :id
            """)
    void markAsFailed(@Param("id") UUID id);
}
