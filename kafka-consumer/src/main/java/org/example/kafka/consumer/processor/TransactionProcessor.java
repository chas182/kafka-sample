package org.example.kafka.consumer.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.kafka.consumer.dto.Transaction;
import org.example.kafka.consumer.entity.TransactionEntity;
import org.example.kafka.consumer.exception.NoClientException;
import org.example.kafka.consumer.repository.ClientEntityRepository;
import org.example.kafka.consumer.repository.TransactionEntityRepository;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionProcessor implements KafkaSampleProcessor {

    private final ClientEntityRepository clientRepository;
    private final TransactionEntityRepository transactionRepository;
    private final ObjectMapper objectMapper;

    @Override
    public void process(String json) throws Exception {
        log.info("Processing transaction: {}", json);
        var transaction = objectMapper.readValue(json, Transaction.class);

        var client = clientRepository.findByClientId(transaction.getClientId());

        if (client.isPresent()) {
            var transactionEntity = convert(transaction);
            transactionEntity.setClient(client.get());

            log.info("Save transaction: {}", transaction);
            transactionRepository.save(transactionEntity);
        } else {
            throw new NoClientException("No client with clientId %s for the transaction %s".formatted(transaction.getClientId(), json));
        }
    }

    public TransactionEntity convert(Transaction source) {
        var entity = new TransactionEntity();

        entity.setBank(source.getBank());
        entity.setOrderType(source.getOrderType());
        entity.setQuantity(source.getQuantity());
        entity.setPrice(source.getPrice());
        entity.setCreatedAt(source.getCreatedAt());

        entity.setCost(source.getPrice() * source.getQuantity());

        return entity;
    }
}
