package org.example.kafka.consumer.integrationtests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.kafka.consumer.domain.TransactionType;
import org.example.kafka.consumer.dto.Client;
import org.example.kafka.consumer.dto.Transaction;
import org.example.kafka.consumer.entity.ClientEntity;
import org.example.kafka.consumer.repository.ClientEntityRepository;
import org.example.kafka.consumer.repository.TransactionEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class KafkaConsumerAppIT {

    private static final String CLIENT_TOPIC = "client-topic";
    private static final String TRANSACTION_TOPIC = "transaction-topic";
    private static final Long CLIENT_ID = 1L;
    private static final String EMAIL = "foo@bar.com";
    private static final String BANK = "PKO";
    private static final Integer QUANTITY = 10;
    private static final Double PRICE = 100.0;
    private static final Double COST = 1000.0;
    private static final LocalDateTime CREATED_AT = LocalDateTime.parse("2024-01-01T00:00:00");

    @Container
    static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            .withEmbeddedZookeeper()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withEnv("KAFKA_CREATE_TOPICS", CLIENT_TOPIC + ":1:1," + TRANSACTION_TOPIC + ":1:1");

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:latest"
    );

    private static Properties buildConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Value("${client.topic}")
    private String clientTopic;
    @Value("${transaction.topic}")
    private String transactionTopic;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private ClientEntityRepository clientRepository;
    @Autowired
    private TransactionEntityRepository transactionRepository;
    @Autowired
    private Producer<Long, String> producer;

    private KafkaConsumer<Long, String> consumer;

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
        jdbcTemplate.execute("truncate table transactions, clients cascade ");
    }

    @Test
    void shouldProcessClient() throws Exception {
        String clientJson = objectMapper.writeValueAsString(client);

        producer.send(new ProducerRecord<>(clientTopic, CLIENT_ID, clientJson));

        var clientEntityOpt = await().atMost(ofSeconds(10))
                .until(() -> clientRepository.findByClientId(CLIENT_ID), Optional::isPresent);

        assertThat(clientEntityOpt).isNotEmpty();
        var clientEntity = clientEntityOpt.get();
        assertThat(clientEntity.getId()).isPositive();
        assertThat(clientEntity.getClientId()).isEqualTo(CLIENT_ID);
        assertThat(clientEntity.getEmail()).isEqualTo(EMAIL);
    }

    @Test
    void shouldProcessTransaction() throws Exception {
        var clientEntity = new ClientEntity();
        clientEntity.setEmail(EMAIL);
        clientEntity.setClientId(CLIENT_ID);
        clientRepository.save(clientEntity);

        String transactionJson = objectMapper.writeValueAsString(transaction);
        producer.send(new ProducerRecord<>(TRANSACTION_TOPIC, CLIENT_ID, transactionJson));

        var transactions = await().atMost(ofSeconds(10))
                .until(() -> transactionRepository.findAll(), res -> res.iterator().hasNext());

        var transactionEntity = transactions.iterator().next();
        assertThat(transactionEntity.getId()).isPositive();
        assertThat(transactionEntity.getBank()).isEqualTo(BANK);
        assertThat(transactionEntity.getPrice()).isEqualTo(PRICE);
        assertThat(transactionEntity.getQuantity()).isEqualTo(QUANTITY);
        assertThat(transactionEntity.getCost()).isEqualTo(COST);
        assertThat(transactionEntity.getCreatedAt()).isEqualTo(CREATED_AT);
        assertThat(transactionEntity.getClient()).isEqualTo(clientEntity);
    }

    @Test
    void shouldNotProcessTransactionWhenClientNotExist() throws Exception {
        consumer = new KafkaConsumer<>(buildConsumerProperties());
        consumer.subscribe(List.of(transactionTopic + "-dlq"));
        String transactionJson = objectMapper.writeValueAsString(transaction);
        producer.send(new ProducerRecord<>(TRANSACTION_TOPIC, CLIENT_ID, transactionJson));

        var dlqRecord = pollForRecord();

        assertThat(dlqRecord).isNotNull();
        assertThat(dlqRecord.key()).isEqualTo(CLIENT_ID);
        assertThat(dlqRecord.value()).isEqualTo(transactionJson);
        consumer.close();
    }

    private ConsumerRecord<Long, String> pollForRecord() {
        ConsumerRecord<Long, String> record = null;
        for (int i = 0; i < 10; i++) {
            var records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        return record;
    }
}
