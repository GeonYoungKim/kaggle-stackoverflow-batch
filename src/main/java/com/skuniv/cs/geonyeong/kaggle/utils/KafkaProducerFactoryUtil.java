package com.skuniv.cs.geonyeong.kaggle.utils;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerFactoryUtil {
    private static Properties producerProperties() throws ConfigurationException {
        String bootstrapServers = YmlUtil.getYmlProps().getProperty("com.skuniv.cs.geonyeong.kaggle.kafka.bootstrapServers");
        String schemaRegistryUrl = YmlUtil.getYmlProps().getProperty("com.skuniv.cs.geonyeong.kaggle.schema.registry.url");

        Properties configProps = new Properties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, Thread.currentThread().getName());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        return configProps;
    }

    public static <T> KafkaProducer<String, T> createKafkaProducer() throws ConfigurationException {
        return new KafkaProducer<String, T>(producerProperties());
    }
}
