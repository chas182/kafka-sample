package org.example.kafka.producer.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.producer.dto.Client;
import org.example.kafka.producer.dto.Transaction;
import org.example.kafka.producer.dto.TransactionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(ProducerResource.class)
class ProducerResourceTest {

    private static final String CLIENT_TOPIC = "client-topic";
    private static final String TRANSACTION_TOPIC = "transaction-topic";
    private static final Long CLIENT_ID = 1L;
    private static final String EMAIL = "foo@bar.com";
    private static final String BANK = "PKO";
    private static final Integer QUANTITY = 1;
    private static final Double PRICE = 100.0;
    private static final LocalDateTime CREATED_AT = LocalDateTime.now();

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private Producer<Long, String> kafkaProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @Captor
    private ArgumentCaptor<ProducerRecord<Long, String>> captor;

    private Client client;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        client = new Client();
        client.setClientId(CLIENT_ID);
        client.setEmail(EMAIL);

        transaction = new Transaction();
        transaction.setBank(BANK);
        transaction.setClientId(CLIENT_ID);
        transaction.setOrderType(TransactionType.INCOME);
        transaction.setQuantity(QUANTITY);
        transaction.setPrice(PRICE);
        transaction.setCreatedAt(CREATED_AT);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(kafkaProducer);
    }

    @Test
    void shouldPostClient() throws Exception {
        String clientJson = objectMapper.writeValueAsString(client);

        mockMvc.perform(post("/client")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(clientJson))
                .andExpect(status().isOk());

        verify(kafkaProducer).send(captor.capture());
        ProducerRecord<Long, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo(CLIENT_TOPIC);
        assertThat(record.key()).isEqualTo(client.getClientId());
        assertThat(record.value()).isEqualTo(clientJson);
    }

    @Test
    void shouldReturnBadRequestOnPostClientWhenClientIsInvalid() throws Exception {
        String clientJson = objectMapper.writeValueAsString(new Client());

        mockMvc.perform(post("/client")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(clientJson))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldPostTransaction() throws Exception {
        String transactionJson = objectMapper.writeValueAsString(transaction);

        mockMvc.perform(post("/transaction")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(transactionJson))
                .andExpect(status().isOk());

        verify(kafkaProducer).send(captor.capture());
        ProducerRecord<Long, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo(TRANSACTION_TOPIC);
        assertThat(record.key()).isEqualTo(transaction.getClientId());
        assertThat(record.value()).isEqualTo(transactionJson);
    }

    @Test
    void shouldReturnBadRequestOnPostTransactionWhenClientIsInvalid() throws Exception {
        String clientJson = objectMapper.writeValueAsString(new Transaction());

        mockMvc.perform(post("/transaction")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(clientJson))
                .andExpect(status().isBadRequest());
    }
}