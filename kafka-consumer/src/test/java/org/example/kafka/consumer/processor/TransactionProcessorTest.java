package org.example.kafka.consumer.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.kafka.consumer.domain.TransactionType;
import org.example.kafka.consumer.dto.Transaction;
import org.example.kafka.consumer.entity.ClientEntity;
import org.example.kafka.consumer.entity.TransactionEntity;
import org.example.kafka.consumer.exception.NoClientException;
import org.example.kafka.consumer.repository.ClientEntityRepository;
import org.example.kafka.consumer.repository.TransactionEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionProcessorTest {

    private static final Long CLIENT_ID = 1L;
    private static final String EMAIL = "foo@bar";
    private static final String BANK = "PKO";
    private static final Integer QUANTITY = 10;
    private static final Double PRICE = 100.0;
    private static final Double COST = 1000.0;
    private static final String TRANSACTION_JSON = "{\"clientId\":1,\"bank\":\"PKO\",\"orderType\":\"INCOME\",\"quantity\":10,\"price\":100.0,\"createdAt\":\"2024-01-01T00:00:00\"}";
    private static final String INVALID_JSON = "invalid json";
    private static final LocalDateTime CREATED_AT = LocalDateTime.parse("2024-01-01T00:00:00");

    @Mock
    private ClientEntityRepository clientRepository;

    @Mock
    private TransactionEntityRepository transactionRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Captor
    ArgumentCaptor<TransactionEntity> transactionEntityCaptor;

    @InjectMocks
    private TransactionProcessor transactionProcessor;

    private Transaction transaction;
    private ClientEntity clientEntity;

    @BeforeEach
    void setUp() {
        clientEntity = new ClientEntity();
        clientEntity.setClientId(CLIENT_ID);
        clientEntity.setEmail(EMAIL);

        transaction = new Transaction();
        transaction.setClientId(CLIENT_ID);
        transaction.setBank(BANK);
        transaction.setOrderType(TransactionType.INCOME);
        transaction.setQuantity(QUANTITY);
        transaction.setPrice(PRICE);
        transaction.setCreatedAt(CREATED_AT);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(clientRepository, transactionRepository);
    }

    @Test
    void shouldProcessValidTransaction() throws Exception {
        when(objectMapper.readValue(TRANSACTION_JSON, Transaction.class)).thenReturn(transaction);
        when(clientRepository.findByClientId(CLIENT_ID)).thenReturn(Optional.of(clientEntity));

        transactionProcessor.process(TRANSACTION_JSON);

        verify(clientRepository).findByClientId(CLIENT_ID);
        verify(transactionRepository).save(transactionEntityCaptor.capture());

        TransactionEntity capturedTransactionEntity = transactionEntityCaptor.getValue();
        assertEquals(BANK, capturedTransactionEntity.getBank());
        assertEquals(clientEntity, capturedTransactionEntity.getClient());
        assertEquals(TransactionType.INCOME, capturedTransactionEntity.getOrderType());
        assertEquals(QUANTITY, capturedTransactionEntity.getQuantity());
        assertEquals(PRICE, capturedTransactionEntity.getPrice());
        assertEquals(COST, capturedTransactionEntity.getCost());
        assertEquals(CREATED_AT, capturedTransactionEntity.getCreatedAt());
    }

    @Test
    void shouldThrowNoClientExceptionWhenClientNotExist() throws Exception {
        when(objectMapper.readValue(TRANSACTION_JSON, Transaction.class)).thenReturn(transaction);
        when(clientRepository.findByClientId(CLIENT_ID)).thenReturn(Optional.empty());

        NoClientException exception = assertThrows(NoClientException.class,
                () -> transactionProcessor.process(TRANSACTION_JSON));

        assertEquals("No client with clientId %d for the transaction %s".formatted(CLIENT_ID, TRANSACTION_JSON), exception.getMessage());
        verify(clientRepository).findByClientId(CLIENT_ID);
        verify(transactionRepository, never()).save(any(TransactionEntity.class));
    }

    @Test
    void shouldThrowExceptionWhenInvalidJsonWasReceived() throws Exception {
        when(objectMapper.readValue(INVALID_JSON, Transaction.class)).thenThrow(new RuntimeException("Boo"));

        assertThrows(RuntimeException.class, () -> transactionProcessor.process(INVALID_JSON));

        verify(clientRepository, never()).findByClientId(anyLong());
        verify(transactionRepository, never()).save(any(TransactionEntity.class));
    }
}
