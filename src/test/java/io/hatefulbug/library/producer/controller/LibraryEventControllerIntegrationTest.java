package io.hatefulbug.library.producer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hatefulbug.library.producer.model.Book;
import io.hatefulbug.library.producer.model.LibraryEvent;
import io.hatefulbug.library.producer.model.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.hatefulbug.library.producer.util.TopicsRepo.LIBRARY_TOPIC;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {LIBRARY_TOPIC}, partitions = 1)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {

        Map<String, Object> props =
                new HashMap<>(
                        KafkaTestUtils.consumerProps(
                                "group1",
                                "true",
                                embeddedKafkaBroker
                        )
                );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        consumer =
                new DefaultKafkaConsumerFactory<String, String>(props)
                        .createConsumer();

        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void postLibraryEvent() throws Exception {

        // given
        Book book = Book.builder()
                .bookId(UUID.randomUUID())
                .title("The Shadow over Innsmouth")
                .author("H.P. Lovecraft")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        HttpEntity<LibraryEvent> httpEntity =
                new HttpEntity<>(libraryEvent, headers);

        // when
        ResponseEntity<LibraryEvent> responseEntity =
                restTemplate.exchange(
                        "/v1/libraryevent",
                        HttpMethod.POST,
                        httpEntity,
                        LibraryEvent.class
                );

        // then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        assertNotNull(responseEntity.getBody());
        assertNotNull(responseEntity.getBody().getLibraryEventId());

        ConsumerRecord<String, String> consumerRecord =
                KafkaTestUtils.getSingleRecord(consumer, LIBRARY_TOPIC);

        assertNotNull(consumerRecord);

        // FIX: deserialize JSON String into LibraryEvent
        LibraryEvent publishedEvent =
                objectMapper.readValue(
                        consumerRecord.value(),
                        LibraryEvent.class
                );

        assertNotNull(publishedEvent);
        assertNotNull(publishedEvent.getLibraryEventId());
        assertNotNull(publishedEvent.getLibraryEventType());

        assertEquals(
                book.getBookId(),
                publishedEvent.getBook().getBookId()
        );

        assertEquals(
                book.getTitle(),
                publishedEvent.getBook().getTitle()
        );

        assertEquals(
                book.getAuthor(),
                publishedEvent.getBook().getAuthor()
        );
    }
}