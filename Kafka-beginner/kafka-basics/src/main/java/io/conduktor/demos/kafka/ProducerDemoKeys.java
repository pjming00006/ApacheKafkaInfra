package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'm a Kafka producer");

        // Create Producer Properties
        Properties properties = new Properties();

        // Connect to local server
        // properties.setProperty("bootstrap.servers", "107.0.0.1:9092");

        // Connect to Conduktor
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2kspA17d4L6AgF0Cn9h02j\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIya3NwQTE3ZDRMNkFnRjBDbjloMDJqIiwib3JnYW5pemF0aW9uSWQiOjczMzgzLCJ1c2VySWQiOjg1MzI4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJkNTMzYTc2Zi1kZGY3LTQ2YjItOTA1NC00YTk2NWYxNjdkN2QifX0.D3WIi0xyyBnZaYOQvOrGMuHIan6xmKpPJTa1Rve5tqk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

//        properties.setProperty("batch.size", "400");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a Record to Send to Kafka
        for (int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hw " + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<String, String>(topic, key, value);

                // Send Record
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("\nkey: " + key +
                                    "| partition: " + metadata.partition());
                        } else {
                            log.error("ERROR");
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Flush Producer
        producer.flush();

        // Flush and Close Producer
        producer.close();
    }
}
