package com.orderflow.outbox;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

interface OrderRowRepository extends JpaRepository<OrderRow, UUID> {
}

interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    // the demo just wants "what did we write recently"
    List<OutboxEvent> findByOrderByCreatedAtDesc(Limit limit);
}
