package com.kaan9898.orderservice.integration;

import com.kaan9898.orderservice.OrderServiceApplication;
import com.kaan9898.orderservice.web.controller.OrderController;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
@AutoConfigureMockMvc
@SpringBootTest(classes = {OrderController.class, OrderServiceApplication.class})
@EmbeddedKafka(partitions = 3, topics = "order-events", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public class OrderIntegrationTest {
    @Autowired
    private MockMvc mockmvc;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    private Consumer<String, String> consumer;
    @BeforeEach
     void setup() {
        Map<String,Object> props = KafkaTestUtils.consumerProps(embeddedKafka,"test-group", true);
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafka.consumeFromEmbeddedTopics(consumer, "order-events");
    }
    @AfterEach
    void cleanup() {
        consumer.close();
    }
    @Test
    void postOrderShouldPublishKafkaEvent() throws Exception {
        String body = """
            {
            "customerId": 15,
            "product": "Laptop",
            "quantity": 2
        }
        """;
        mockmvc.perform(post("/order/create").contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated());
        ConsumerRecord<String, String> consumerRecords = KafkaTestUtils
                .getSingleRecord(consumer,"order-events", Duration.ofSeconds(10));
        assertThat(consumerRecords.key()).isEqualTo("15");
        assertThat(consumerRecords.value()).contains("Laptop").contains("ORDER_CREATED");
    }
}
