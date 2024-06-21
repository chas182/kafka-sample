package org.example.kafka.consumer.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.example.kafka.consumer.processor.KafkaSampleProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

    private static final String TOPIC = "test-topic";
    private static final String DLQ_TOPIC = TOPIC + "-dlq";
    private static final String MESSAGE = "valid message";
    private static final String INVALID_MESSAGE = "invalid message";
    private static final Long RECORD_KEY = 1L;

    @Mock
    private Consumer<Long, String> consumer;

    @Mock
    private Producer<Long, String> producer;

    @Mock
    private KafkaSampleProcessor processor;

    @Captor
    ArgumentCaptor<ProducerRecord<Long, String>> producerRecordCaptor;

    private KafkaConsumerService kafkaConsumerService;
    

    @BeforeEach
    void setUp() {
        kafkaConsumerService = new KafkaConsumerService(consumer, TOPIC, processor, producer);
        doNothing().when(consumer).wakeup();
        doNothing().when(consumer).close();
        doNothing().when(producer).close();
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(consumer, producer, processor);
    }

    @Test
    void shouldProcessValidMessage() throws Exception {
        var record = new ConsumerRecord<>(TOPIC, 0, 0L, RECORD_KEY, MESSAGE);
        var records = new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 0), List.of(record)));

        when(consumer.poll(any(Duration.class))).thenReturn(records).thenAnswer(invocation -> {
            kafkaConsumerService.shutdown();
            return ConsumerRecords.empty();
        });

        var thread = new Thread(() -> kafkaConsumerService.startConsuming());
        thread.start();
        thread.join();

        verify(consumer, timeout(1000)).subscribe(List.of(TOPIC));
        verify(processor, timeout(1000)).process(MESSAGE);
    }

    @Test
    void shouldProcessInvalidMessageAndSendMessageToDlq() throws Exception {
        var record = new ConsumerRecord<>(TOPIC, 0, 0L, RECORD_KEY, INVALID_MESSAGE);
        var records = new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 0), List.of(record)));

        doThrow(new Exception("Processing failed")).when(processor).process(INVALID_MESSAGE);
        when(consumer.poll(any(Duration.class))).thenReturn(records).thenAnswer(invocation -> {
            kafkaConsumerService.shutdown();
            return ConsumerRecords.empty();
        });

        var thread = new Thread(() -> kafkaConsumerService.startConsuming());
        thread.start();
        thread.join();

        verify(consumer, timeout(1000)).subscribe(List.of(TOPIC));
        verify(processor, timeout(1000)).process(INVALID_MESSAGE);

        verify(producer, timeout(1000)).send(producerRecordCaptor.capture());

        ProducerRecord<Long, String> capturedRecord = producerRecordCaptor.getValue();
        assertEquals(DLQ_TOPIC, capturedRecord.topic());
        assertEquals(RECORD_KEY, capturedRecord.key());
        assertEquals(INVALID_MESSAGE, capturedRecord.value());
    }
}
