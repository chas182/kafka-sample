package org.example.kafka.consumer.repository;

import org.example.kafka.consumer.entity.ClientEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface ClientEntityRepository extends CrudRepository<ClientEntity, Long> {
    Optional<ClientEntity> findByClientId(Long clientId);
    boolean existsByClientId(Long clientId);
}
