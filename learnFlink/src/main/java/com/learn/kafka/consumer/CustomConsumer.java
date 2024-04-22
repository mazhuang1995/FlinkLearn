package com.learn.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class CustomConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);


    }
}
