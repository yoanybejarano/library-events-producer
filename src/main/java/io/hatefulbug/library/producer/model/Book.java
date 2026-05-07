package io.hatefulbug.library.producer.model;

import lombok.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {
    @NotNull
    private UUID bookId;
    @NotBlank
    private String title;
    @NotBlank
    private String author;
}
