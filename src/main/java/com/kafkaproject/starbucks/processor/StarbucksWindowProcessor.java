package com.kafkaproject.starbucks.processor;

import com.kafkaproject.starbucks.consumer.DebeziumJsonHandler;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.serdes.JsonSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StarbucksWindowProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksWindowProcessor.class);

    // Вхідні теми
    private static final String DB_TOPIC = "postgres-server.public.starbucks";
    private static final String STREAM_TOPIC = "starbucks-stream";

    // Вихідні теми
    private static final String HIGH_CALORIE_COUNT_TOPIC = "starbucks-high-calorie-count";

    private static final String HIGH_CALORIE_STORE = "starbucks-high-calorie-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "starbucks-stateful-processor");
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

        // Фільтруємо та об'єднуємо по ключу
        KGroupedStream<String, Double> groupedStream = mergedStream
                .filter((key, product) -> product.getCalories() > 200)
                .map((k, v) -> KeyValue.pair("high_calorie", v.getCalories()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        // Операції зі збереженням стану з різними типами вікон.
        Serde<Windowed<String>> timeWindowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10_000);

        // 1. Вікна фіксованого розміру (тумбочні, без перекриття)
        groupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.as(HIGH_CALORIE_STORE + "-tumbling"))
                .toStream()
                .to(HIGH_CALORIE_COUNT_TOPIC + "-tumbling", Produced.with(timeWindowedSerde, Serdes.Long()));

        // 2. Вікна фіксованого розміру з перекриттям (hopping)
        groupedStream
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(5)))
                .count(Materialized.as(HIGH_CALORIE_STORE + "-hopping"))
                .toStream()
                .to(HIGH_CALORIE_COUNT_TOPIC + "-hopping", Produced.with(timeWindowedSerde, Serdes.Long()));

        groupedStream
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.as(HIGH_CALORIE_STORE + "-sliding"))
                .toStream()
                .to(HIGH_CALORIE_COUNT_TOPIC + "-sliding", Produced.with(timeWindowedSerde, Serdes.Long()));

        groupedStream
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.as(HIGH_CALORIE_STORE + "-session"))
                .toStream()
                .to(HIGH_CALORIE_COUNT_TOPIC + "-session",
                        Produced.with(WindowedSerdes.sessionWindowedSerdeFrom(String.class), Serdes.Long()));

        // Логування обробки
        mergedStream.foreach((key, product) ->
                logger.info("Обробка продукту: {}, Калорії: {}",
                        product.getProductName(), product.getCalories()));

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
            logger.info("Обробку потоків з операціями з вікнами розпочато");
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Обробку потоків призупинено: ", e);
        } finally {
            streams.close();
        }
    }
}