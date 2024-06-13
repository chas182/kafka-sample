package org.example.kafka.producer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.producer.dto.Client;
import org.example.kafka.producer.dto.Transaction;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class ProducerResource {

    private static final String CLIENT_TOPIC = "client-topic";
    private static final String TRANSACTION_TOPIC = "transaction-topic";

    private final Producer<Long, String> kafkaProducer;
    private final ObjectMapper objectMapper;

    @PostMapping(value = "client", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void postClient(@RequestBody @Valid Client client) {
        var record = new ProducerRecord<>(CLIENT_TOPIC, client.getClientId(), toJson(client));
        kafkaProducer.send(record);
        log.info("Sent client: {}", client);
    }

    @PostMapping(value = "transaction", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void postTransaction(@RequestBody @Valid Transaction transaction) {
        var record = new ProducerRecord<>(TRANSACTION_TOPIC, transaction.getClientId(), toJson(transaction));
        kafkaProducer.send(record);
        log.info("Sent transaction: {}", transaction);
    }

    @SneakyThrows
    private String toJson(Object client) {
        return objectMapper.writeValueAsString(client);
    }
}
