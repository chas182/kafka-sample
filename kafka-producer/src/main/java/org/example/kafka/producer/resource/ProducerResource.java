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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class ProducerResource {

    @Value("${client.topic}")
    private final String clientTopic;
    @Value("${transaction.topic}")
    private final String transactionTopic;

    private final Producer<Long, String> kafkaProducer;
    private final ObjectMapper objectMapper;

    @PostMapping(value = "client", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void postClient(@RequestBody @Valid Client client) {
        var record = new ProducerRecord<>(clientTopic, client.getClientId(), toJson(client));
        kafkaProducer.send(record);
        log.info("Sent client: {}", client);
    }

    @PostMapping(value = "transaction", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void postTransaction(@RequestBody @Valid Transaction transaction) {
        var record = new ProducerRecord<>(transactionTopic, transaction.getClientId(), toJson(transaction));
        kafkaProducer.send(record);
        log.info("Sent transaction: {}", transaction);
    }

    @SneakyThrows
    private String toJson(Object client) {
        return objectMapper.writeValueAsString(client);
    }
}
