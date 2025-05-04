package com.kafkaproject.starbucks.producer;

import com.kafkaproject.starbucks.model.StarbucksProduct;

import java.util.Random;

public class StarbucksDataGenerator {
    private static final Random random = new Random();

    private static final String[] PRODUCT_NAMES = {
            "Caffe Latte", "Caffe Mocha", "White Chocolate Mocha",
            "Cappuccino", "Caramel Macchiato", "Flat White",
            "Espresso", "Americano", "Cold Brew", "Frappuccino",
            "Iced Coffee", "Chai Tea Latte", "Green Tea Latte"
    };

    private static final String[] SIZES = {
            "short", "tall", "grande", "venti"
    };

    public static StarbucksProduct generateRandomProduct() {
        StarbucksProduct product = new StarbucksProduct();

        product.setProductName(PRODUCT_NAMES[random.nextInt(PRODUCT_NAMES.length)]);
        product.setSize(SIZES[random.nextInt(SIZES.length)]);
        product.setMilk(random.nextInt(6));
        product.setWhip(random.nextInt(2));

        String size = product.getSize();
        double sizeFactor = switch (size) {
            case "Short" -> 0.7;
            case "Tall" -> 1.0;
            case "Grande" -> 1.3;
            case "Venti" -> 1.6;
            default -> 0;
        };

        product.setServSizeMl(240 * sizeFactor);
        product.setCalories(120 + random.nextInt(150) * sizeFactor);
        product.setTotalFatG(3 + random.nextInt(10) * sizeFactor);
        product.setSaturatedFatG(1 + random.nextInt(5) * sizeFactor);
        product.setTransFatG(random.nextDouble() < 0.2 ? "0.5" : "0");
        product.setCholesterolMg(10 + random.nextInt(20) * sizeFactor);
        product.setSodiumMg(100 + random.nextInt(150) * sizeFactor);
        product.setTotalCarbsG(15 + random.nextInt(25) * sizeFactor);
        product.setFiberG(random.nextDouble() < 0.3 ? "1" : "0");
        product.setSugarG(10 + random.nextInt(20) * sizeFactor);
        product.setCaffeineMg(75 + random.nextInt(100) * sizeFactor);

        return product;
    }
}