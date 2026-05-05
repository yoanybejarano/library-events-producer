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
public class Book {
    private UUID bookId;
    private String title;
    private String author;
}
