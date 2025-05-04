package com.kafkaproject.starbucks.processor;

import com.kafkaproject.starbucks.consumer.DebeziumJsonHandler;
import com.kafkaproject.starbucks.model.JoinedProduct;
import com.kafkaproject.starbucks.model.OrderType;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.model.VolumeInfo;
import com.kafkaproject.starbucks.serdes.JsonSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StarbucksStatefulProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StarbucksStatefulProcessor.class);

    // Вхідні теми
    private static final String DB_TOPIC = "postgres-server.public.starbucks";
    private static final String STREAM_TOPIC = "starbucks-stream";

    // Вихідні теми
    private static final String HIGH_CALORIE_COUNT_TOPIC = "starbucks-high-calorie-count";
    private static final String NO_MILK_CALORIES_TOPIC = "starbucks-no-milk-calories";
    private static final String ORDER_TYPE_TOPIC = "starbucks-order-type";
    private static final String VOLUME_INFO_TOPIC = "starbucks-volume-info";
    private static final String JOINED_PRODUCTS_TOPIC = "starbucks-joined-products";

    private static final String HIGH_CALORIE_STORE = "starbucks-high-calorie-store";
    private static final String NO_MILk_CALORIE_STORE = "total-no-milk-calories-store";


    public static void main(String[] args) {
        // Налаштування для Kafka Streams
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

        // 1. Порахувати кількість напоїв, у яких понад 200 ккал
        KTable<String, Long> highCalorieCount = mergedStream
                .filter((key, product) -> product.getCalories() > 200)
                .map((k, v) -> KeyValue.pair("high_calorie", v.getCalories()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .count(Materialized.as(HIGH_CALORIE_STORE));
        highCalorieCount.toStream().to(HIGH_CALORIE_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        // 2. Порахувати кількість калорій у напоях без молока
        KTable<String, Double> noMilkCalorieCount = mergedStream
                .filter((key, product) -> product.getMilk() == 0)
                .map((k, v) -> KeyValue.pair("no_milk_calorie", v.getCalories()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce(
                        Double::sum,
                        Materialized.as(NO_MILk_CALORIE_STORE)
                );
        noMilkCalorieCount.toStream().to(NO_MILK_CALORIES_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));

        // 3. Розділити дані за допомогою операції розгалудження
        // 3.1 Створюємо новий потік для демонстрації розгалуження і join
        // Використаємо генератор даних, який вже є
        KStream<String, StarbucksProduct> generatedStream = mergedStream
                .filter((key, product) -> key != null);

        // Розділяємо потік на два: тип замовлення та об'єм у замовленні
        KStream<String, OrderType> orderTypeStream = generatedStream
                .mapValues(product -> {
                    OrderType orderType = new OrderType();
                    orderType.setProductName(product.getProductName());
                    orderType.setSize(product.getSize());
                    orderType.setMilk(product.getMilk());
                    orderType.setWhip(product.getWhip());
                    return orderType;
                });

        KStream<String, VolumeInfo> volumeInfoStream = generatedStream
                .mapValues(product -> {
                    VolumeInfo volumeInfo = new VolumeInfo();
                    volumeInfo.setServSizeMl(product.getServSizeMl());
                    volumeInfo.setCalories(product.getCalories());
                    volumeInfo.setTotalFatG(product.getTotalFatG());
                    volumeInfo.setSaturatedFatG(product.getSaturatedFatG());
                    volumeInfo.setTransFatG(product.getTransFatG());
                    volumeInfo.setCholesterolMg(product.getCholesterolMg());
                    volumeInfo.setSodiumMg(product.getSodiumMg());
                    volumeInfo.setTotalCarbsG(product.getTotalCarbsG());
                    volumeInfo.setFiberG(product.getFiberG());
                    volumeInfo.setSugarG(product.getSugarG());
                    volumeInfo.setCaffeineMg(product.getCaffeineMg());
                    return volumeInfo;
                });

        // Записуємо дані в окремі теми
        orderTypeStream.to(ORDER_TYPE_TOPIC,
                Produced.with(Serdes.String(), JsonSerdes.OrderType()));

        volumeInfoStream.to(VOLUME_INFO_TOPIC,
                Produced.with(Serdes.String(), JsonSerdes.VolumeInfo()));

        // 3.2 Демонстрація join операції
        // Змінюємо ключі для join
        KStream<String, OrderType> keyedOrderTypeStream = orderTypeStream
                .selectKey((oldKey, orderType) -> oldKey);

        KStream<String, VolumeInfo> keyedVolumeInfoStream = volumeInfoStream
                .selectKey((oldKey, volumeInfo) -> oldKey);

        // Виконуємо join
        ValueJoiner<OrderType, VolumeInfo, JoinedProduct> joiner =
                (orderType, volumeInfo) -> {
                    JoinedProduct joined = new JoinedProduct();
                    joined.setProductName(orderType.getProductName());
                    joined.setSize(orderType.getSize());
                    joined.setMilk(orderType.getMilk());
                    joined.setWhip(orderType.getWhip());
                    joined.setServSizeMl(volumeInfo.getServSizeMl());
                    joined.setCalories(volumeInfo.getCalories());
                    joined.setTotalFatG(volumeInfo.getTotalFatG());
                    joined.setSaturatedFatG(volumeInfo.getSaturatedFatG());
                    joined.setTransFatG(volumeInfo.getTransFatG());
                    joined.setCholesterolMg(volumeInfo.getCholesterolMg());
                    joined.setSodiumMg(volumeInfo.getSodiumMg());
                    joined.setTotalCarbsG(volumeInfo.getTotalCarbsG());
                    joined.setFiberG(volumeInfo.getFiberG());
                    joined.setSugarG(volumeInfo.getSugarG());
                    joined.setCaffeineMg(volumeInfo.getCaffeineMg());
                    return joined;
                };

        // Використовуємо stream-stream join з вікном
        KStream<String, JoinedProduct> joinedStream = keyedOrderTypeStream.join(
                keyedVolumeInfoStream,
                joiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(java.time.Duration.ofMinutes(5)),
                StreamJoined.with(
                        Serdes.String(),
                        JsonSerdes.OrderType(),
                        JsonSerdes.VolumeInfo()
                )
        );

        // Записуємо результат join в окрему тему
        joinedStream.to(JOINED_PRODUCTS_TOPIC,
                Produced.with(Serdes.String(), JsonSerdes.JoinedProduct()));

        // Логування обробки
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
            logger.info("Обробку потоків з збереженням стану розпочато");
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Обробку потоків призупинено: ", e);
        } finally {
            streams.close();
        }
    }
}