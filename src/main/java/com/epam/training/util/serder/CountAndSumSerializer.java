package com.epam.training.util.serder;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.epam.training.util.CountAndSum;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CountAndSumSerializer implements Serializer<CountAndSum> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, CountAndSum data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, CountAndSum data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
