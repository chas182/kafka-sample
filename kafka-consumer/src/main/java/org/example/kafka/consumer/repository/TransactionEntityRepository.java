package org.example.kafka.consumer.repository;

import org.example.kafka.consumer.entity.TransactionEntity;
import org.springframework.data.repository.CrudRepository;

public interface TransactionEntityRepository extends CrudRepository<TransactionEntity, Long> {
}
