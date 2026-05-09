package io.hatefulbug.library.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hatefulbug.library.producer.model.Book;
import io.hatefulbug.library.producer.model.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.hatefulbug.library.producer.util.TopicsRepo.LIBRARY_TOPIC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper;

    Book book;
    LibraryEvent libraryEvent;

    @BeforeEach
    void setUp() {
        book = Book.builder()
                .bookId(UUID.randomUUID())
                .title("Kafka using Spring Boot")
                .author("Yoany")
                .build();

        libraryEvent = LibraryEvent.builder()
                .libraryEventId(UUID.randomUUID())
                .book(book)
                .build();
    }

    @Test
    void sendLibraryEvent_Approach2_failure() {

        // given
        CompletableFuture<SendResult<String, String>> completableFuture =
                new CompletableFuture<>();

        RuntimeException exception = new RuntimeException("Kafka failure");

        completableFuture.completeExceptionally(exception);

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(completableFuture);

        // when + then
        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                () -> libraryEventProducer
                        .sendLibraryEvent_Approach2(libraryEvent)
                        .get()
        );

        assertEquals(
                "Kafka failure",
                executionException.getCause().getMessage()
        );

        verify(kafkaTemplate, times(1))
                .send(any(ProducerRecord.class));
    }

    @Test
    void sendLibraryEvent_Approach2() throws Exception {

        // given
        String key = libraryEvent.getLibraryEventId().toString();

        String value = objectMapper.writeValueAsString(libraryEvent);

        RecordMetadata recordMetadata = mock(RecordMetadata.class);

        SendResult<String, String> sendResult =
                new SendResult<>(
                        new ProducerRecord<>(LIBRARY_TOPIC, key, value),
                        recordMetadata
                );

        CompletableFuture<SendResult<String, String>> completableFuture =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(completableFuture);

        // when
        CompletableFuture<SendResult<String, String>> result =
                libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

        // then
        assertNotNull(result);

        verify(kafkaTemplate, times(1))
                .send(any(ProducerRecord.class));

        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, String> producerRecord = captor.getValue();

        assertEquals(LIBRARY_TOPIC, producerRecord.topic());

        // key should now be UUID string
        assertEquals(key, producerRecord.key());

        // value should be serialized JSON
        assertEquals(value, producerRecord.value());

        // optional: verify custom header
        Header header = producerRecord.headers()
                .lastHeader("event-source");

        assertNotNull(header);

        assertEquals(
                "scanner",
                new String(header.value())
        );
    }
}