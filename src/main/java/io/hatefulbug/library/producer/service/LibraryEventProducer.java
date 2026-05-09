package io.hatefulbug.library.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        String value = objectMapper.writeValueAsString(libraryEvent);
        CompletableFuture<SendResult<String, String>> completableFuture = kafkaTemplate.send(LIBRARY_TOPIC, key, value);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(ex);
            } else {
                handleSuccess(key, value, result);
            }
        });
    }

    public SendResult<String, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<String, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.send(LIBRARY_TOPIC, key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;
    }

    public CompletableFuture<SendResult<String, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {

        if (libraryEvent.getLibraryEventId() == null) {
            libraryEvent.setLibraryEventId(UUID.randomUUID());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<String, String> producerRecord = buildProducerRecord(key, value, LIBRARY_TOPIC);
        CompletableFuture<SendResult<String, String>> completableFuture =  kafkaTemplate.send(producerRecord);
        completableFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                handleFailure(ex);
            } else {
                handleSuccess(key, value, result);
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

    private void handleSuccess(String key, String value, SendResult<String, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value id is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }

    private ProducerRecord<String, String> buildProducerRecord(String key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

}
