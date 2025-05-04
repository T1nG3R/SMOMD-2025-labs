package com.kafkaproject.starbucks.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DebeziumJsonHandler {
    private static final Logger logger = LoggerFactory.getLogger(DebeziumJsonHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static StarbucksProduct parseDebeziumJson(String jsonStr) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonStr);
            
            // Перевіряємо наявність payload і його атрибутів
            if (rootNode.has("payload") && rootNode.get("payload").has("after")) {
                JsonNode afterNode = rootNode.get("payload").get("after");
                
                // Мапимо поля з JSON на об'єкт StarbucksProduct
                StarbucksProduct product = new StarbucksProduct();
                
                if (afterNode.has("product_name")) {
                    product.setProductName(afterNode.get("product_name").asText());
                }
                
                if (afterNode.has("size")) {
                    product.setSize(afterNode.get("size").asText());
                }
                
                if (afterNode.has("milk")) {
                    product.setMilk(afterNode.get("milk").asDouble());
                }
                
                if (afterNode.has("whip")) {
                    product.setWhip(afterNode.get("whip").asDouble());
                }
                
                if (afterNode.has("serv_size_ml")) {
                    product.setServSizeMl(afterNode.get("serv_size_ml").asDouble());
                }
                
                if (afterNode.has("calories")) {
                    product.setCalories(afterNode.get("calories").asDouble());
                }
                
                if (afterNode.has("total_fat_g")) {
                    product.setTotalFatG(afterNode.get("total_fat_g").asDouble());
                }
                
                if (afterNode.has("saturated_fat_g")) {
                    product.setSaturatedFatG(afterNode.get("saturated_fat_g").asDouble());
                }
                
                if (afterNode.has("trans_fat_g")) {
                    product.setTransFatG(afterNode.get("trans_fat_g").asText());
                }
                
                if (afterNode.has("cholesterol_mg")) {
                    product.setCholesterolMg(afterNode.get("cholesterol_mg").asDouble());
                }
                
                if (afterNode.has("sodium_mg")) {
                    product.setSodiumMg(afterNode.get("sodium_mg").asDouble());
                }
                
                if (afterNode.has("total_carbs_g")) {
                    product.setTotalCarbsG(afterNode.get("total_carbs_g").asDouble());
                }
                
                if (afterNode.has("fiber_g")) {
                    product.setFiberG(afterNode.get("fiber_g").asText());
                }
                
                if (afterNode.has("sugar_g")) {
                    product.setSugarG(afterNode.get("sugar_g").asDouble());
                }
                
                if (afterNode.has("caffeine_mg")) {
                    product.setCaffeineMg(afterNode.get("caffeine_mg").asDouble());
                }
                
                return product;
            }
            
            logger.warn("JSON не містить необхідних полів: {}", jsonStr);
            return null;
        } catch (IOException e) {
            logger.error("Помилка розбору JSON: ", e);
            return null;
        }
    }
}