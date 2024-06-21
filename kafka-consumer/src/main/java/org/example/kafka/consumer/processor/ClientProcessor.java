package org.example.kafka.consumer.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.kafka.consumer.dto.Client;
import org.example.kafka.consumer.entity.ClientEntity;
import org.example.kafka.consumer.repository.ClientEntityRepository;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClientProcessor implements KafkaSampleProcessor {

    private final ClientEntityRepository clientRepository;
    private final ObjectMapper objectMapper;

    @Override
    public void process(String json) throws Exception {
        log.info("Processing client: {}", json);
        var client = objectMapper.readValue(json, Client.class);

        var clientExist = clientRepository.existsByClientId(client.getClientId());

        if (!clientExist) {
            log.info("Save client {}", client);
            clientRepository.save(convert(client));
        } else {
            log.warn("Client with id {} already exist, skip", client.getClientId());
        }
    }

    public ClientEntity convert(Client source) {
        var entity = new ClientEntity();

        entity.setClientId(source.getClientId());
        entity.setEmail(source.getEmail());

        return entity;
    }
}
