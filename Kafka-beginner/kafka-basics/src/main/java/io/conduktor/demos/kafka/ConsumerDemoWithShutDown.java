package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Kafka consumer");

        String groupId = "my_java_application";
        String topic = "demo_java";

        // Create Consumer Properties
        Properties properties = new Properties();

        // Connect to local server
        // properties.setProperty("bootstrap.servers", "127.0.1:9092");

        // Connect to Conduktor
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2kspA17d4L6AgF0Cn9h02j\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya3NwQTE3ZDRMNkFnRjBDbjloMDJqIiwib3JnYW5pemF0aW9uSWQiOjczMzgzLCJ1c2VySWQiOjg1MzI4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJkNTMzYTc2Zi1kZGY3LTQ2YjItOTA1NC00YTk2NWYxNjdkN2QifX0.D3WIi0xyyBnZaYOQvOrGMuHIan6xmKpPJTa1Rve5tqk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        // Set Consumer Properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // none/earliest/latest
        // none: if there is no existing consumer group, then the program fails
        // earliest: read from the beginning of the topic
        // latest: read from the end of the topic
        properties.setProperty("auto.offset.reset", "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Get a reference for the main thread
        final Thread mainThread = Thread.currentThread();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown. Let's exit by calling consumer.wakeup");
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulling");
                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + " value: " + record.value() + "\npartition: " + record.partition() +
                            " offset: " + record.offset() + "\n");
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is shutting down...");
        } catch(Exception e) {
            log.error("Unexpected error in the consumer", e);
        } finally {
            // Close consumer and commit offsets
            consumer.close();
            log.info("The consumer is now gracefully shut down.");
        }
    }
}
