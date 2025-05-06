package com.kafkaproject.starbucks.processor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerMonitoring implements MetricsReporter {
    private static final Logger logger = LoggerFactory.getLogger(ProducerMonitoring.class);

    // Тема для моніторингових даних
    private static final String MONITORING_TOPIC = "starbucks-monitoring";

    private static final int REPORTING_INTERVAL_SECONDS = 10;
    private final List<KafkaMetric> metrics = new ArrayList<>();
    private KafkaProducer<String, String> producer;
    private ScheduledExecutorService scheduler;

    @Override
    public void init(List<KafkaMetric> metrics) {
        this.metrics.addAll(metrics);

        // Ініціалізація продюсера для надсилання метрик
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "metrics-reporter");

        this.producer = new KafkaProducer<>(producerProps);

        // Запуск планувальника для регулярного відправлення метрик
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::reportMetrics,
                REPORTING_INTERVAL_SECONDS, REPORTING_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        // Додаємо нову або оновлену метрику
        synchronized (metrics) {
            // Перевіряємо, чи метрика вже існує, та оновлюємо її
            boolean exists = false;
            for (int i = 0; i < metrics.size(); i++) {
                if (metrics.get(i).metricName().equals(metric.metricName())) {
                    metrics.set(i, metric);
                    exists = true;
                    break;
                }
            }

            // Якщо метрика нова, додаємо її
            if (!exists) {
                metrics.add(metric);
            }
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // Видаляємо метрику, що більше не відстежується
        synchronized (metrics) {
            metrics.removeIf(m -> m.metricName().equals(metric.metricName()));
        }
    }

    @Override
    public void close() {
        // Зупиняємо планувальник і закриваємо продюсера
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (producer != null) {
            producer.close();
        }

        logger.info("ProducerMonitoring закрито");
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    private void reportMetrics() {
        try {
            synchronized (metrics) {
                if (metrics.isEmpty()) {
                    return;
                }

                // Фільтруємо найважливіші метрики
                List<KafkaMetric> producerMetrics = metrics.stream().filter(this::isProducerMetric).toList();

                long timestamp = System.currentTimeMillis();

                // Відправляємо кожну метрику як окремий запис
                for (KafkaMetric metric : producerMetrics) {
                    MetricName name = metric.metricName();

                    // Перевіряємо, чи метрика має коректне значення
                    Object value = metric.metricValue();
                    if (value == null) {
                        continue;
                    }

                    // Формуємо окремий JSON для кожної метрики
                    String metricJson = String.format(
                            "{\"timestamp\":%d,\"name\":\"%s\",\"group\":\"%s\",\"value\":%s}",
                            timestamp, name.name(), name.group(), value
                    );

                    // Відправляємо метрику в тему моніторингу
                    producer.send(new ProducerRecord<>(MONITORING_TOPIC, name.group() + "." + name.name(), metricJson),
                            (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Помилка відправки метрики {}: {}", name.name(), exception.getMessage());
                        }
                    });
                }

                logger.info("Відправлено {} метрик в тему {}", producerMetrics.size(), MONITORING_TOPIC);
            }
        } catch (Exception e) {
            logger.error("Помилка під час обробки та відправки метрик: {}", e.getMessage(), e);
        }
    }

    private boolean isProducerMetric(KafkaMetric metric) {
        String group = metric.metricName().group();

        return group.contains("producer-metrics");
    }
}