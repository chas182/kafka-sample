package org.example.kafka.consumer.processor;

public interface KafkaSampleProcessor {
    void process(String json) throws Exception;
}
