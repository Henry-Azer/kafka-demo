package org.henry.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;
    Logger logger;

    public ConsumerRunnable(Logger logger, CountDownLatch latch, KafkaConsumer<String, String> consumer) {
        this.logger = logger;
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> logger.info("Key: " + record.key() + " Value: " + record.value() + " Partition: " + record.partition() + " Offset: " + record.offset()));
            }
        } catch (WakeupException wakeupException) {
            logger.info("Shutdown poll signal!");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        // interrupt poll method
        // throw wakeup exception
        consumer.wakeup();
    }
}
