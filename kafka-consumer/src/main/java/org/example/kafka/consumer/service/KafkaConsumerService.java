package org.example.kafka.consumer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.consumer.processor.KafkaSampleProcessor;

import java.time.Duration;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final Consumer<Long, String> consumer;
    private final String topic;
    private final KafkaSampleProcessor processor;
    private final Producer<Long, String> producer;

    private volatile boolean running = true;

    @PostConstruct
    public void startConsuming() {
        consumer.subscribe(List.of(topic));
        log.info("Starting consumer for {} topic", topic);

        new Thread(() -> {
            log.info("Starting consuming for {} topic", topic);
            while (running) {
                try {
                    ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Long, String> record : records) {
                        try {
                            processor.process(record.value());
                        } catch (Exception e) {
                            log.error("Error while consuming from topic {}:", topic, e);
                            sendToDlq(record);
                        }
                    }
                } catch (Exception e) {
                    if (running) {
                        throw e; // If exception occurs while running, propagate it
                    }
                }
            }
        }, topic + "-consumer").start();
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        consumer.wakeup(); // Wake up the consumer to exit the poll loop
        consumer.close();
        producer.close();
    }

    private void sendToDlq(ConsumerRecord<Long, String> record) {
        var dlqRecord = new ProducerRecord<>(record.topic() + "-dlq", record.key(), record.value());
        producer.send(dlqRecord);
    }
}
