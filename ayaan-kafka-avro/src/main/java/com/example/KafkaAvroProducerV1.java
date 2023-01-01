package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "20.231.198.226:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://20.231.198.226:8081");

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        String topic = "customer-avro";

        // copied from avro examples
        Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(34)
                .setHeight(178f)
                .setWeight(75f)
                .setAutomatedEmail(false)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(
                topic, customer
        );

        System.out.println(customer);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();

    }
}

