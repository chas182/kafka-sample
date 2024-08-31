package org.example.kafka.producer.intergrationtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.kafka.producer.dto.Client;
import org.example.kafka.producer.dto.Transaction;
import org.example.kafka.producer.dto.TransactionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class KafkaProducerAppIT {

    private static final String CLIENT_TOPIC = "client-topic";
    private static final String TRANSACTION_TOPIC = "transaction-topic";
    private static final Long CLIENT_ID = 1L;
    private static final String EMAIL = "foo@bar.com";
    private static final String BANK = "PKO";
    private static final Integer QUANTITY = 1;
    private static final Double PRICE = 100.0;
    private static final LocalDateTime CREATED_AT = LocalDateTime.now();

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            .withEmbeddedZookeeper()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withEnv("KAFKA_CREATE_TOPICS", CLIENT_TOPIC + ":1:1," + TRANSACTION_TOPIC + ":1:1");

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
    }

    @Autowired
    private ObjectMapper objectMapper;

    @LocalServerPort
    private Integer port;

    private KafkaConsumer<Long, String> consumer;

    private Client client;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        RestAssured.baseURI = "http://localhost:" + port;

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

        consumer = new KafkaConsumer<>(buildConsumerProperties());
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldPostClient() throws Exception {
        String clientJson = objectMapper.writeValueAsString(client);

        consumer.subscribe(List.of(CLIENT_TOPIC));
        given()
                .contentType(ContentType.JSON)
                .body(clientJson)
                .when()
                .post("/client")
                .then()
                .statusCode(200);


        ConsumerRecord<Long, String> record = pollForRecord();

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(client.getClientId());
        assertThat(record.value()).isEqualTo(clientJson);
    }

    @Test
    void shouldPostTransaction() throws Exception {
        String transactionJson = objectMapper.writeValueAsString(transaction);

        consumer.subscribe(List.of(TRANSACTION_TOPIC));
        given()
                .contentType(ContentType.JSON)
                .body(transactionJson)
                .when()
                .post("/transaction")
                .then()
                .statusCode(200);

        ConsumerRecord<Long, String> record = pollForRecord();

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(transaction.getClientId());
        assertThat(record.value()).isEqualTo(transactionJson);
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
