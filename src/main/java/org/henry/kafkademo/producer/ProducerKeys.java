package org.henry.kafkademo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerKeys.class);

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // add more than record
        for (int i = 0; i < 8; i++) {
            String topic = "test_topic";
            String value = "kafka message " + i;
            String key = "id_" + i;

            logger.info("Key: " + key);

            // create record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

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
            }).get();

        }
        // asynchronous
        producer.flush();
        producer.close();
    }
}