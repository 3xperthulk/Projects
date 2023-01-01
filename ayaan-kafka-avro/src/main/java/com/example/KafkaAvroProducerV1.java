package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAvroProducerV1 {
//    public static final Logger log = LoggerFactory.getLogger(KafkaAvroProducerV1.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "20.168.204.33:29092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://20.168.204.33:8081");

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
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                Logger log = LoggerFactory.getLogger(KafkaAvroProducerV1.class);
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();

    }
}

