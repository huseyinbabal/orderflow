package com.orderflow.es;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

interface EsEventRepository extends JpaRepository<EsEvent, Long> {

    List<EsEvent> findByAggregateIdOrderByVersionAsc(UUID aggregateId);
}
