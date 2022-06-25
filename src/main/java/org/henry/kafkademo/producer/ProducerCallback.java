package org.henry.kafkademo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String TOPIC = "test_topic";
        String VALUE = "kafka message";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, VALUE);

        // send record with callback
        producer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                logger.info("Metadata: \n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp() + "\n"
                );
            } else {
                logger.error("Error: " + exception);
            }
        });

        // asynchronous
        producer.flush();
        producer.close();
    }
}