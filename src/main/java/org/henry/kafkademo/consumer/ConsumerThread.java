package org.henry.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GROUP_ID = "my-first-app";
        // offsets reset: earliest - latest - none
        String OFFSET_RESET = "earliest";
        String TOPIC = "test_topic";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topics
        consumer.subscribe(List.of(TOPIC));

        // runnable consumer
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable runnableConsumer = new ConsumerRunnable(logger, latch, consumer);

        // start thread
        Thread thread = new Thread(runnableConsumer);
        thread.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    runnableConsumer.shutdown();

                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    logger.info("Application exited.");
                }
        ));
        // latch await
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application interrupted! ", e);
        } finally {
            logger.info("Application closed.");
        }


    }
}
