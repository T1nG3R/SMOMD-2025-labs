package com.kafkaproject.starbucks.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaproject.starbucks.model.JoinedProduct;
import com.kafkaproject.starbucks.model.OrderType;
import com.kafkaproject.starbucks.model.StarbucksProduct;
import com.kafkaproject.starbucks.model.VolumeInfo;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonSerdes {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerdes.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class StarbucksProductSerializer implements Serializer<StarbucksProduct> {
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
                logger.error("Помилка серіалізації StarbucksProduct: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static class StarbucksProductDeserializer implements Deserializer<StarbucksProduct> {
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

    public static class OrderTypeSerializer implements Serializer<OrderType> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, OrderType data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                logger.error("Помилка серіалізації OrderType: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static class OrderTypeDeserializer implements Deserializer<OrderType> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public OrderType deserialize(String topic, byte[] data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, OrderType.class);
            } catch (Exception e) {
                logger.error("Помилка десеріалізації даних у OrderType: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static Serde<OrderType> OrderType() {
        return new Serde<>() {
            @Override
            public Serializer<OrderType> serializer() {
                return new OrderTypeSerializer();
            }

            @Override
            public Deserializer<OrderType> deserializer() {
                return new OrderTypeDeserializer();
            }
        };
    }
    
    public static class VolumeInfoSerializer implements Serializer<VolumeInfo> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, VolumeInfo data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                logger.error("Помилка серіалізації VolumeInfo: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static class VolumeInfoDeserializer implements Deserializer<VolumeInfo> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public VolumeInfo deserialize(String topic, byte[] data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, VolumeInfo.class);
            } catch (Exception e) {
                logger.error("Помилка десеріалізації даних у VolumeInfo: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static Serde<VolumeInfo> VolumeInfo() {
        return new Serde<>() {
            @Override
            public Serializer<VolumeInfo> serializer() {
                return new VolumeInfoSerializer();
            }

            @Override
            public Deserializer<VolumeInfo> deserializer() {
                return new VolumeInfoDeserializer();
            }
        };
    }

    public static class JoinedProductSerializer implements Serializer<JoinedProduct> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, JoinedProduct data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                logger.error("Помилка серіалізації JoinedProduct: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static class JoinedProductDeserializer implements Deserializer<JoinedProduct> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public JoinedProduct deserialize(String topic, byte[] data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.readValue(data, JoinedProduct.class);
            } catch (Exception e) {
                logger.error("Помилка десеріалізації даних у JoinedProduct: ", e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    public static Serde<JoinedProduct> JoinedProduct() {
        return new Serde<>() {
            @Override
            public Serializer<JoinedProduct> serializer() {
                return new JoinedProductSerializer();
            }

            @Override
            public Deserializer<JoinedProduct> deserializer() {
                return new JoinedProductDeserializer();
            }
        };
    }

    public static Serde<StarbucksProduct> StarbucksProduct() {
        return new Serde<>() {
            @Override
            public Serializer<StarbucksProduct> serializer() {
                return new StarbucksProductSerializer();
            }

            @Override
            public Deserializer<StarbucksProduct> deserializer() {
                return new StarbucksProductDeserializer();
            }
        };
    }
}