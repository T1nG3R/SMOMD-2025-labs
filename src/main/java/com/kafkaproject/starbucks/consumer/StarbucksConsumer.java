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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class StarbucksConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "starbucks-consumer-group";
    private static final String DB_TOPIC = "postgres-server.public.starbucks";
    private static final String STREAM_TOPIC = "starbucks-stream";

    public static void main(String[] args) {
        // Налаштування для споживача власних даних
        Properties streamProps = new Properties();
        streamProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + "-stream");
        streamProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        streamProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StarbucksDeserializer.class.getName());
        streamProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        streamProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Налаштування для споживача даних з БД
        Properties dbProps = new Properties();
        dbProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        dbProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + "-db");
        dbProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        dbProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        dbProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dbProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        dbProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Створюємо та запускаємо Consumer для власних потокових даних в окремому потоці
        Thread streamConsumerThread = new Thread(() -> {
            try (KafkaConsumer<String, StarbucksProduct> consumer = new KafkaConsumer<>(streamProps)) {
                consumer.subscribe(List.of(STREAM_TOPIC));
                logger.info("Підписано на тему: {}", STREAM_TOPIC);

                while (true) {
                    ConsumerRecords<String, StarbucksProduct> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, StarbucksProduct> record : records) {
                        StarbucksProduct product = record.value();

                        if (product != null) {
                            logger.info("Потоковий запис: Ключ={}, Розділ={}, Зсув={}", record.key(), record.partition(), record.offset());
                            logger.info("Продукт: {}", product);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Помилка у споживачі потокових даних: ", e);
            }
        });

        // Створюємо та запускаємо Consumer для даних з PostgreSQL в окремому потоці
        Thread dbConsumerThread = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(dbProps)) {
                consumer.subscribe(List.of(DB_TOPIC));
                logger.info("Підписано на тему: {}", DB_TOPIC);

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        String jsonStr = record.value();

                        if (jsonStr != null) {
                            logger.info("БД запис: Ключ={}, Розділ={}, Зсув={}", record.key(), record.partition(), record.offset());

                            // Обробка JSON з Debezium
                            StarbucksProduct product = DebeziumJsonHandler.parseDebeziumJson(jsonStr);
                            if (product != null) {
                                logger.info("Продукт з БД: {}", product);
                            } else {
                                // Якщо не вдалося розпарсити, виводимо весь JSON
                                logger.info("Оригінальний JSON: {}", jsonStr);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Помилка у споживачі даних з БД: ", e);
            }
        });

        // Запускаємо обидва потоки
        streamConsumerThread.start();
        dbConsumerThread.start();

        logger.info("Споживачі запущено. Натисніть Ctrl+C для виходу...");

        // Приєднуємо потоки
        try {
            streamConsumerThread.join();
            dbConsumerThread.join();
        } catch (InterruptedException e) {
            logger.error("Виконання перервано", e);
        }
    }
}