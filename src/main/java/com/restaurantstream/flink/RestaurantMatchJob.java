package com.restaurantstream.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RestaurantMatchJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> queries = env.addSource(
                new KinesisPollingSource("restaurant-queries"), "kinesis-queries-source");

        queries
                .map(new RestaurantMatcher())
                .addSink(new KinesisSink("restaurant-results"));

        env.execute("restaurant-match-job");
    }
}
