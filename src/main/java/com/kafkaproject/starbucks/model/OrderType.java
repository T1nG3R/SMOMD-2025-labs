package com.kafkaproject.starbucks.model;

public class OrderType {
    private String productName;
    private String size;
    private double milk;
    private double whip;

    public OrderType() {
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public double getMilk() {
        return milk;
    }

    public void setMilk(double milk) {
        this.milk = milk;
    }

    public double getWhip() {
        return whip;
    }

    public void setWhip(double whip) {
        this.whip = whip;
    }

    @Override
    public String toString() {
        return "OrderType{" + "productName='" + productName + '\'' + ", size='" + size + '\'' + ", milk=" + milk + ", whip=" + whip + '}';
    }
}