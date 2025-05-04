package com.kafkaproject.starbucks.consumer;

import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.serdes.StarbucksDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StarbucksProcessedConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksProcessedConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "starbucks-processed-consumer-group";

    // Теми для споживання
    private static final String HIGH_CALORIE_TOPIC = "starbucks-high-calorie";
    private static final String NO_MILK_TOPIC = "starbucks-no-milk";
    private static final String COCONUT_MILK_TOPIC = "starbucks-coconut-milk";
    private static final String OTHER_MILK_TOPIC = "starbucks-other-milk";

    public static void main(String[] args) {
        // Налаштування для споживачів
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StarbucksDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Створюємо теми для різних типів продуктів
        Thread highCalorieThread = createConsumerThread(props, HIGH_CALORIE_TOPIC, "Висококалорійні продукти");

        Thread noMilkThread = createConsumerThread(props, NO_MILK_TOPIC, "Продукти без молока");

        Thread coconutMilkThread = createConsumerThread(props, COCONUT_MILK_TOPIC, "Продукти з косовим молоком");

        Thread otherMilkThread = createConsumerThread(props, OTHER_MILK_TOPIC, "Інший тип молока");

        // Запускаємо потоки
        highCalorieThread.start();
        noMilkThread.start();
        coconutMilkThread.start();
        otherMilkThread.start();

        logger.info("Споживачі запущено. Натисніть Ctrl+C для виходу...");

        // Приєднуємо потоки
        try {
            highCalorieThread.join();
            noMilkThread.join();
            coconutMilkThread.join();
            otherMilkThread.join();
        } catch (InterruptedException e) {
            logger.error("Виконання перервано", e);
        }
    }

    private static Thread createConsumerThread(Properties props, String topic, String consumerName) {
        return new Thread(() -> {
            try (KafkaConsumer<String, StarbucksProduct> consumer = new KafkaConsumer<>(props)) {

                consumer.subscribe(Collections.singletonList(topic));
                logger.info("{} підписано на тему: {}", consumerName, topic);

                while (true) {
                    ConsumerRecords<String, StarbucksProduct> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, StarbucksProduct> record : records) {
                        StarbucksProduct product = record.value();

                        if (product != null) {
                            logger.info("{} - Ключ={}, Розділ={}, Зсув={}", consumerName, record.key(), record.partition(), record.offset());
                            logger.info("{} - Продукт: {}", consumerName, product);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Помилка у споживачі {}: ", consumerName, e);
            }
        });
    }
}