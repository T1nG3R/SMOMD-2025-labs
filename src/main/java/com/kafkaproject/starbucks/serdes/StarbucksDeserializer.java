package com.kafkaproject.starbucks.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StarbucksDeserializer implements Deserializer<StarbucksProduct> {
    private final Logger logger = LoggerFactory.getLogger(StarbucksDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public StarbucksProduct deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.readValue(data, StarbucksProduct.class);
        } catch (Exception e) {
            logger.error("Помилка десеріалізації даних у StarbucksProduct: ", e);
            return null;
        }
    }

    @Override
    public void close() {
    }
}