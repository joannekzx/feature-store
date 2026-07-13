package com.featurestore.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

// connect to Kafka on localhost:9092
// generate 100 fake events, one every 500ms
// each event has a random entityId, random amount, random eventType, and current timestamp
// serialises each event to json and sends it to the events topic

public class EventProducer {
    private static final String TOPIC = "events";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        System.out.println("Starting producer");

        for (int i = 0; i < 100; i++) {
            String entityId = "user-" + random.nextInt(10);
            double amount = Math.round(random.nextDouble() * 1000 * 100.0) / 100.0;
            int eventType = random.nextInt(3) + 1;
            long timestamp = System.currentTimeMillis();

            Event event = new Event(entityId, amount, eventType, timestamp);
            String json = mapper.writeValueAsString(event);

            producer.send(new ProducerRecord<>(TOPIC, entityId, json));
            System.out.println("Sent: " + json);

            Thread.sleep(500);
        }
        producer.close();
        System.out.println("Done.");
    }
}
