package receive.ReceiveService.Config;


import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    //create a new topic for saving the faulty messages


    @Bean
    public NewTopic topic(){
        return TopicBuilder.name("product-test-topic.DLT")
                .partitions(3)
                .replicas(1)
                .build();

    }


    //producer factory is used inorder to create producer instances such that using this config
    //the producer is used in order to publish the messages in to kafka DLT
    @Bean
    public ProducerFactory<String,Object> factory(){
        Map<String,Object> config = new HashMap<String,Object>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }


    @Bean
    public KafkaTemplate<String,Object> kafkaTemplate(){
        return new KafkaTemplate<>(factory());
    }




    @Bean
   public DefaultErrorHandler errorHandler(KafkaTemplate<String,Object> template){
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,(record,exception)->new TopicPartition(record.topic()+".DLT",record.partition() )
        );

       FixedBackOff backOff = new FixedBackOff(1000L,2);
       return new DefaultErrorHandler(recoverer,backOff);
   }





}
