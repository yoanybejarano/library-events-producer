package io.hatefulbug.library.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {

    @Bean
    public NewTopic newTopic() {
        return TopicBuilder.name("library_events")
                .partitions(1)
                .replicas(1)
                .build();
    }



}
