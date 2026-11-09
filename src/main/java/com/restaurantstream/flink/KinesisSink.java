package com.restaurantstream.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class KinesisSink extends RichSinkFunction<String> {

    private final String streamName;
    private transient KinesisClient client;

    public KinesisSink(String streamName) {
        this.streamName = streamName;
    }

    @Override
    public void open(Configuration parameters) {
        client = KinesisConfig.buildClient();
    }

    @Override
    public void invoke(String value, Context context) {
        client.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(UUID.randomUUID().toString())
                .data(SdkBytes.fromByteArray(value.getBytes(StandardCharsets.UTF_8)))
                .build());
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
