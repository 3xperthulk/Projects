package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAvroProducerV1 {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "20.232.151.242:19092,20.232.151.242:29092,20.232.151.242:39092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://20.232.151.242:8081");

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        String topic = "customer-avro";

        // copied from avro examples
        Customer customer = Customer.newBuilder()
                .setFirstName("Laurel")
                .setLastName("Newman")
                .setAge(34)
                .setHeight(178f)
                .setWeight(75f)
                .setAutomatedEmail(false)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(
                topic, customer
        );

        System.out.println(customer);
        for (int i = 1; i <= 10000000; i++) {
            producer.send(producerRecord, (metadata, exception) -> {
                Logger log = LoggerFactory.getLogger(KafkaAvroProducerV1.class);
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            });

        }
        producer.flush();
        producer.close();

    }
}

