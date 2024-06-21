package org.example.kafka.consumer.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.kafka.consumer.dto.Client;
import org.example.kafka.consumer.entity.ClientEntity;
import org.example.kafka.consumer.repository.ClientEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientProcessorTest {

    private static final Long CLIENT_ID = 1L;
    private static final String EMAIL = "foo@bar.com";
    private static final String CLIENT_JSON = "{\"clientId\":1,\"email\":\"foo@bar.com\"}";
    private static final String INVALID_JSON = "invalid json";

    @Mock
    private ClientEntityRepository clientRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Captor
    ArgumentCaptor<ClientEntity> clientEntityCaptor;

    @InjectMocks
    private ClientProcessor clientProcessor;

    private Client client;

    @BeforeEach
    void setUp() {
        client = new Client();
        client.setClientId(CLIENT_ID);
        client.setEmail(EMAIL);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(clientRepository);
    }

    @Test
    void shouldProcessNewClient() throws Exception {
        when(objectMapper.readValue(CLIENT_JSON, Client.class)).thenReturn(client);
        when(clientRepository.existsByClientId(CLIENT_ID)).thenReturn(false);

        clientProcessor.process(CLIENT_JSON);

        verify(clientRepository).existsByClientId(CLIENT_ID);
        verify(clientRepository).save(clientEntityCaptor.capture());

        ClientEntity capturedClientEntity = clientEntityCaptor.getValue();
        assertEquals(CLIENT_ID, capturedClientEntity.getClientId());
        assertEquals(EMAIL, capturedClientEntity.getEmail());
    }

    @Test
    void shouldProcessExistingClient() throws Exception {
        when(objectMapper.readValue(CLIENT_JSON, Client.class)).thenReturn(client);
        when(clientRepository.existsByClientId(CLIENT_ID)).thenReturn(true);

        clientProcessor.process(CLIENT_JSON);

        verify(clientRepository).existsByClientId(CLIENT_ID);
        verify(clientRepository, never()).save(any(ClientEntity.class));
    }

    @Test
    void shouldThrowExceptionWhenInvalidJsonWasReceived() throws Exception {
        when(objectMapper.readValue(INVALID_JSON, Client.class)).thenThrow(new RuntimeException("Boo"));

        assertThrows(RuntimeException.class, () -> clientProcessor.process(INVALID_JSON));

        verify(clientRepository, never()).existsByClientId(anyLong());
        verify(clientRepository, never()).save(any(ClientEntity.class));
    }
}
