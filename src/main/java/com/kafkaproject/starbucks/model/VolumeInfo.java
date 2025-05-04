package com.kafkaproject.starbucks.model;

public class VolumeInfo {
    private double servSizeMl;
    private double calories;
    private double totalFatG;
    private double saturatedFatG;
    private String transFatG;
    private double cholesterolMg;
    private double sodiumMg;
    private double totalCarbsG;
    private String fiberG;
    private double sugarG;
    private double caffeineMg;

    public VolumeInfo() {
    }

    public double getServSizeMl() {
        return servSizeMl;
    }

    public void setServSizeMl(double servSizeMl) {
        this.servSizeMl = servSizeMl;
    }

    public double getCalories() {
        return calories;
    }

    public void setCalories(double calories) {
        this.calories = calories;
    }

    public double getTotalFatG() {
        return totalFatG;
    }

    public void setTotalFatG(double totalFatG) {
        this.totalFatG = totalFatG;
    }

    public double getSaturatedFatG() {
        return saturatedFatG;
    }

    public void setSaturatedFatG(double saturatedFatG) {
        this.saturatedFatG = saturatedFatG;
    }

    public String getTransFatG() {
        return transFatG;
    }

    public void setTransFatG(String transFatG) {
        this.transFatG = transFatG;
    }

    public double getCholesterolMg() {
        return cholesterolMg;
    }

    public void setCholesterolMg(double cholesterolMg) {
        this.cholesterolMg = cholesterolMg;
    }

    public double getSodiumMg() {
        return sodiumMg;
    }

    public void setSodiumMg(double sodiumMg) {
        this.sodiumMg = sodiumMg;
    }

    public double getTotalCarbsG() {
        return totalCarbsG;
    }

    public void setTotalCarbsG(double totalCarbsG) {
        this.totalCarbsG = totalCarbsG;
    }

    public String getFiberG() {
        return fiberG;
    }

    public void setFiberG(String fiberG) {
        this.fiberG = fiberG;
    }

    public double getSugarG() {
        return sugarG;
    }

    public void setSugarG(double sugarG) {
        this.sugarG = sugarG;
    }

    public double getCaffeineMg() {
        return caffeineMg;
    }

    public void setCaffeineMg(double caffeineMg) {
        this.caffeineMg = caffeineMg;
    }

    @Override
    public String toString() {
        return "VolumeInfo{" + "servSizeMl=" + servSizeMl + ", calories=" + calories + ", totalFatG=" + totalFatG + ", saturatedFatG=" + saturatedFatG + ", transFatG='" + transFatG + '\'' + ", cholesterolMg=" + cholesterolMg + ", sodiumMg=" + sodiumMg + ", totalCarbsG=" + totalCarbsG + ", fiberG='" + fiberG + '\'' + ", sugarG=" + sugarG + ", caffeineMg=" + caffeineMg + '}';
    }
}