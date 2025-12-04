package com.pms.transactional.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.pms.transactional.TradeProto;
import com.pms.transactional.TransactionProto;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic createTopic1() {
        return TopicBuilder.name("transactions-topic")
                .partitions(5)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createTopic2() {
        return TopicBuilder.name("validatedtrades-topic")
                .partitions(5)
                .replicas(1)
                .build();
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    @Bean
    public ProducerFactory<String, ?> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean // default + universal template
    public KafkaTemplate<String, ?> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // @Bean
    // public ProducerFactory<String, TransactionProto> producerFactory() {
    // return new DefaultKafkaProducerFactory<>(producerConfigs());
    // }

    // @Bean(name = "transactionKafkaTemplate")
    // public KafkaTemplate<String, TransactionProto> kafkaTemplate() {
    // return new KafkaTemplate<>(producerFactory());
    // }

    // @Bean
    // public ProducerFactory<String, TradeProto> tradeProducerFactory() {
    // return new DefaultKafkaProducerFactory<>(producerConfigs());
    // }

    // @Bean(name = "tradeKafkaTemplate")
    // public KafkaTemplate<String, TradeProto> tradeKafkaTemplate() {
    // return new KafkaTemplate<>(tradeProducerFactory());
    // }

}
