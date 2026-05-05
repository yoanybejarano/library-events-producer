package io.hatefulbug.library.producer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    private UUID libraryEventId;
    private LibraryEventType libraryEventType;
    private Book book;
}
