package com.restaurantstream.flink;

public class Restaurant {
    public String name;
    public String location;
    public String cuisines;
    public String restType;
    public String rate;
    public String approxCost;

    public Restaurant(String name, String location, String cuisines, String restType, String rate, String approxCost) {
        this.name = name;
        this.location = location;
        this.cuisines = cuisines;
        this.restType = restType;
        this.rate = rate;
        this.approxCost = approxCost;
    }

    String searchableText() {
        return (name + " " + location + " " + cuisines + " " + restType).toLowerCase();
    }
}
