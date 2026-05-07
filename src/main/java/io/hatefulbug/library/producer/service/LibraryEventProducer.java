package io.hatefulbug.library.producer.service;

import io.hatefulbug.library.producer.model.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.hatefulbug.library.producer.util.TopicsRepo.LIBRARY_TOPIC;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventProducer {

    private final KafkaTemplate<String, LibraryEvent> kafkaTemplate;

    public void sendLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        CompletableFuture<SendResult<String, LibraryEvent>> completableFuture = kafkaTemplate.send(LIBRARY_TOPIC, key, libraryEvent);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(ex);
            } else {
                handleSuccess(key, libraryEvent, result);
            }
        });
    }

    public SendResult<String, LibraryEvent> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, TimeoutException {
        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        SendResult<String, LibraryEvent> sendResult = null;
        try {
            sendResult = kafkaTemplate.send(LIBRARY_TOPIC, key, libraryEvent).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;
    }

    public CompletableFuture<SendResult<String, LibraryEvent>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        ProducerRecord<String, LibraryEvent> producerRecord = buildProducerRecord(key, libraryEvent, LIBRARY_TOPIC);
        CompletableFuture<SendResult<String, LibraryEvent>> completableFuture =  kafkaTemplate.send(producerRecord);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(ex);
            } else {
                handleSuccess(key, libraryEvent, result);
            }
        });
        return completableFuture;
    }


    private void handleFailure(Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(String key, LibraryEvent libraryEvent, SendResult<String, LibraryEvent> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value id is {} , partition is {}", key, libraryEvent.getLibraryEventId(), result.getRecordMetadata().partition());
    }

    private ProducerRecord<String, LibraryEvent> buildProducerRecord(String key, LibraryEvent libraryEvent, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, libraryEvent, recordHeaders);
    }

}
