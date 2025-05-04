package com.kafkaproject.starbucks.processor;

import com.kafkaproject.starbucks.consumer.DebeziumJsonHandler;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.serdes.JsonSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StarbucksStreamsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksStreamsProcessor.class);

    // Вхідні теми
    private static final String DB_TOPIC = "postgres-server.public.starbucks";
    private static final String STREAM_TOPIC = "starbucks-stream";

    // Вихідні теми
    private static final String HIGH_CALORIE_TOPIC = "starbucks-high-calorie";
    private static final String NO_MILK_TOPIC = "starbucks-no-milk";
    private static final String COCONUT_MILK_TOPIC = "starbucks-coconut-milk";
    private static final String OTHER_MILK_TOPIC = "starbucks-other-milk";

    public static void main(String[] args) {
        // Налаштування для Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-streams-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Обробляємо дані Debezium з БД
        KStream<String, String> dbStream = builder.stream(DB_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Конвертуємо Debezium JSON в StarbucksProduct
        KStream<String, StarbucksProduct> dbProductStream = dbStream
                .mapValues(DebeziumJsonHandler::parseDebeziumJson)
                .filter((key, product) -> product != null);

        // Обробляємо дані з нашого застосунку виробника 
        KStream<String, StarbucksProduct> directStream = builder.stream(STREAM_TOPIC,
                Consumed.with(Serdes.String(), JsonSerdes.StarbucksProduct()));

        // Об'єднуємо обидва потоки
        KStream<String, StarbucksProduct> mergedStream = dbProductStream.merge(directStream);

        // Відфільтровуємо записи про напої, у яких понад 200 ккал
        KStream<String, StarbucksProduct> highCalorieStream = mergedStream
                .filter((key, product) -> product.getCalories() > 200);

        // Записуємо висококалорійні напої в тему
        highCalorieStream.to(HIGH_CALORIE_TOPIC,
                Produced.with(Serdes.String(), JsonSerdes.StarbucksProduct()));

        // Розділяємо за типом молока
        Map<String, KStream<String, StarbucksProduct>> branches = mergedStream.split(Named.as("milk-")).branch((key, product) -> product.getMilk() == 0, Branched.as("no-milk")).branch((key, product) -> product.getMilk() == 4, Branched.as("coconut-milk")).defaultBranch(Branched.as("other-milk"));

        KStream<String, StarbucksProduct> noMilkStream = branches.get("milk-no-milk");
        KStream<String, StarbucksProduct> coconutMilkStream = branches.get("milk-coconut-milk");
        KStream<String, StarbucksProduct> otherMilkStream = branches.get("milk-other-milk");

        // Відправляємо кожну гілку у відповідну тему
        noMilkStream.to(NO_MILK_TOPIC, Produced.with(Serdes.String(), JsonSerdes.StarbucksProduct()));
        coconutMilkStream.to(COCONUT_MILK_TOPIC, Produced.with(Serdes.String(), JsonSerdes.StarbucksProduct()));
        otherMilkStream.to(OTHER_MILK_TOPIC, Produced.with(Serdes.String(), JsonSerdes.StarbucksProduct()));

        // Логування оброблених записів
        mergedStream.foreach((key, product) ->
                logger.info("Обробка продукту: {}, Калорії: {}, Молоко: {}",
                        product.getProductName(), product.getCalories(), product.getMilk()));

        // Створюємо KafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Додаємо ShutdownHook
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            logger.info("Обробку потоків Starbucks розпочато");
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Обробку потоків Starbucks призупинено: ", e);
        } finally {
            streams.close();
        }
    }
}