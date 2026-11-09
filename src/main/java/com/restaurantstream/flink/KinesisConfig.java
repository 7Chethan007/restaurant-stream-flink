package com.restaurantstream.flink;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.net.URI;

class KinesisConfig {
    static final String ENDPOINT = System.getenv().getOrDefault("KINESIS_ENDPOINT", "http://floci:4566");
    static final String REGION = System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1");

    static KinesisClient buildClient() {
        return KinesisClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();
    }
}
