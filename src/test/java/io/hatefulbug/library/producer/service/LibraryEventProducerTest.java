package io.hatefulbug.library.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hatefulbug.library.producer.model.Book;
import io.hatefulbug.library.producer.model.LibraryEvent;
import io.hatefulbug.library.producer.model.LibraryEventType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.hatefulbug.library.producer.util.TopicsRepo.LIBRARY_TOPIC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Mock
    KafkaTemplate<String, LibraryEvent> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper;

    Book book;
    LibraryEvent libraryEvent;

    @BeforeEach
    void setUp() {

        book = Book.builder()
                .bookId(UUID.randomUUID())
                .title("The Shadow over Innsmouth")
                .author("H.P. Lovecraft")
                .build();

        libraryEvent = LibraryEvent.builder()
                .libraryEventId(UUID.randomUUID())
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
    }

    @Test
    void sendLibraryEvent_Approach2_failure() {

        CompletableFuture<SendResult<String, LibraryEvent>> completableFuture =
                new CompletableFuture<>();

        RuntimeException exception = new RuntimeException("Kafka failure");

        completableFuture.completeExceptionally(exception);

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(completableFuture);

        ExecutionException executionException = assertThrows(
                ExecutionException.class,
                () -> libraryEventProducer
                        .sendLibraryEvent_Approach2(libraryEvent)
                        .get()
        );

        assertEquals("Kafka failure",
                executionException.getCause().getMessage());

        verify(kafkaTemplate, times(1))
                .send(any(ProducerRecord.class));
    }

    @Test
    void sendLibraryEvent_Approach2() {

        //given
        String key = libraryEvent.getLibraryEventId().toString();

        RecordMetadata recordMetadata = mock(RecordMetadata.class);

        SendResult<String, LibraryEvent> sendResult =
                new SendResult<>(
                        new ProducerRecord<>(LIBRARY_TOPIC, key, libraryEvent),
                        recordMetadata
                );

        CompletableFuture<SendResult<String, LibraryEvent>> completableFuture =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(completableFuture);

        //when
        CompletableFuture<SendResult<String, LibraryEvent>> result =
                libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

        //then
        assertNotNull(result);

        verify(kafkaTemplate, times(1))
                .send(any(ProducerRecord.class));

        ArgumentCaptor<ProducerRecord<String, LibraryEvent>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, LibraryEvent> producerRecord = captor.getValue();

        assertEquals(LIBRARY_TOPIC, producerRecord.topic());
        assertEquals(key, producerRecord.key());
        assertEquals(libraryEvent, producerRecord.value());

    }
}