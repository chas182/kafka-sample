package org.example.kafka.consumer.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafka.consumer.processor.ClientProcessor;
import org.example.kafka.consumer.processor.TransactionProcessor;
import org.example.kafka.consumer.service.KafkaConsumerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class AppConfig {

    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;
    @Value("${client.topic}")
    private String clientTopic;
    @Value("${client.consumer.group}")
    private String clientConsumerGroup;
    @Value("${transaction.topic}")
    private String transactionTopic;
    @Value("${transaction.consumer.group}")
    private String transactionConsumerGroup;

    @Bean
    public Consumer<Long, String> clientConsumer() {
        var props = getProperties(clientConsumerGroup);
        return new KafkaConsumer<>(props);
    }

    @Bean
    public Consumer<Long, String> transactionConsumer() {
        var props = getProperties(transactionConsumerGroup);
        return new KafkaConsumer<>(props);
    }

    @Bean
    public KafkaConsumerService clientsConsumerService(ClientProcessor clientProcessor) {
        return new KafkaConsumerService(clientConsumer(), clientTopic, clientProcessor, kafkaProducer());
    }

    @Bean
    public KafkaConsumerService transactionsConsumerService(TransactionProcessor transactionProcessor) {
        return new KafkaConsumerService(transactionConsumer(), transactionTopic, transactionProcessor, kafkaProducer());
    }

    @Bean
    public Producer<Long, String> kafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private Properties getProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
