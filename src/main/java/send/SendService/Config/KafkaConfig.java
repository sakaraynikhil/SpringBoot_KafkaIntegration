package send.SendService.Config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic topic(){
        return TopicBuilder.name("product-test-topic")
                .partitions(3)
                .replicas(1)
                .configs(Map.of("min.insync.replicas","1"))
                .build();
    }

}
