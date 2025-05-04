package com.kafkaproject.starbucks.producer;

import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.serdes.StarbucksSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class StarbucksProducer {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksProducer.class);
    private static final String TOPIC_NAME = "starbucks-stream";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Налаштування Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StarbucksSerializer.class.getName());

        // Додаткові налаштування
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Найвищий рівень надійності
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // Кількість повторних спроб
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // Розмір пакета
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // Час очікування перед надсиланням
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // Розмір буфера

        // Створення Producer
        try (KafkaProducer<String, StarbucksProduct> producer = new KafkaProducer<>(props)) {
            System.out.println("Генерація випадкових даних Starbucks...");
            int numRecords = 3;
            if (args.length > 0) {
                try {
                    numRecords = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    logger.warn("Невірний формат аргументу. Використовуємо значення за замовчуванням: 3");
                }
            }

            // Генерація та надсилання даних
            for (int i = 0; i < numRecords; i++) {
                String key = UUID.randomUUID().toString();
                StarbucksProduct product = StarbucksDataGenerator.generateRandomProduct();

                ProducerRecord<String, StarbucksProduct> record =
                        new ProducerRecord<>(TOPIC_NAME, key, product);

                try {
                    RecordMetadata metadata = producer.send(record).get();
                    logger.info("Відправлено запис: ключ={}, продукт={}, розділ={}, зсув={}",
                            key, product.getProductName(), metadata.partition(), metadata.offset());

                    Thread.sleep(1000);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Помилка при відправці запису", e);
                }
            }

            logger.info("Завершено відправку {} записів", numRecords);
        }
    }
}