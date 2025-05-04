package com.kafkaproject.starbucks.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StarbucksSerializer implements Serializer<StarbucksProduct> {
    private final Logger logger = LoggerFactory.getLogger(StarbucksSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, StarbucksProduct data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            logger.error("Помилка серіалізації об'єкта StarbucksProduct: ", e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}