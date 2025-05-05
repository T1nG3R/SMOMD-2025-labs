package com.kafkaproject.starbucks.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class StarbucksWindowConsumer {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksWindowConsumer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "starbucks-consumer-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // Спочатку читаємо ключ як byte[] — десеріалізуємо самостійно
        KafkaConsumer<byte[], Long> consumer = new KafkaConsumer<>(props);

        List<String> topics = Arrays.asList(
                "starbucks-high-calorie-count-tumbling"
                ,
                "starbucks-high-calorie-count-hopping"
                ,
                "starbucks-high-calorie-count-sliding"
                ,
                "starbucks-high-calorie-count-session"
        );
        consumer.subscribe(topics);

        // Десеріалізатори для різних типів вікон
        TimeWindowedDeserializer<String> timeWindowedDeserializer =
                new TimeWindowedDeserializer<>(new StringDeserializer(), 10_000L);
        SessionWindowedDeserializer<String> sessionWindowedDeserializer =
                new SessionWindowedDeserializer<>(new StringDeserializer());

        logger.info("Споживач запущено. Натисніть Ctrl+C для виходу...");

        // Додаємо механізм перехоплення сигналу завершення
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Отримано сигнал завершення, закриваємо споживача...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Помилка при очікуванні завершення головного потоку", e);
            }
        }));

        try {
            while (true) {
                try {
                    ConsumerRecords<byte[], Long> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<byte[], Long> record : records) {
                        Windowed<String> windowedKey;
                        String topic = record.topic();

                        try {
                            if (topic.contains("session")) {
                                windowedKey = sessionWindowedDeserializer.deserialize(topic, record.key());
                            } else {
                                windowedKey = timeWindowedDeserializer.deserialize(topic, record.key());
                            }

                            logger.info("Тема: {}, Вікно: [{} - {}], Ключ: {}, Кількість: {}, Зсув: {}",
                                    topic,
                                    windowedKey.window().startTime(),
                                    windowedKey.window().endTime(),
                                    windowedKey.key(),
                                    record.value(),
                                    record.offset()
                            );
                        } catch (Exception e) {
                            logger.error("Помилка десеріалізації ключа для топіка {}: {}", topic, e.getMessage());
                        }
                    }
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    // Ігноруємо це виключення, яке виникає при виклику wakeup()
                    logger.info("Отримано WakeupException - підготовка до завершення");
                    break;
                } catch (Exception e) {
                    logger.error("Неочікувана помилка в циклі споживання", e);
                }
            }
        } finally {
            try {
                consumer.close();
                logger.info("Споживач закрито");
            } catch (Exception e) {
                logger.error("Помилка при закритті споживача", e);
            }
        }
    }
}