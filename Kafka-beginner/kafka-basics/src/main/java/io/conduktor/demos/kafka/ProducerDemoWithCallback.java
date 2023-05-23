package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.1:9092");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a Record to Send to Kafka
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("demo_java", "hello my world");

        // Send Record
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e==null){
                    log.info("Received new metadata \n" + "topic: " + metadata.topic());
                }
                else {
                    log.error("ERROR");
                }
            }
        });

        // Flush Producer
        producer.flush();

        // Flush and Close Producer
        producer.close();
    }
}
