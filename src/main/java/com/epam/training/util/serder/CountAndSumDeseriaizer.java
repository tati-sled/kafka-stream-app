package com.epam.training.util.serder;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import com.epam.training.util.CountAndSum;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CountAndSumDeseriaizer implements Deserializer<CountAndSum> {

    public CountAndSumDeseriaizer(Class<CountAndSum> clazz) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public CountAndSum deserialize(String topic, byte[] data) {
        String[] stringData = new String(data, StandardCharsets.UTF_8).split(" ");
        return new CountAndSum(Long.parseLong(stringData[0]), Double.parseDouble(stringData[1]));
    }

    @Override
    public CountAndSum deserialize(String topic, Headers headers, byte[] data) {
        String[] stringData = new String(data, StandardCharsets.UTF_8).split(" ");
        return new CountAndSum(Long.parseLong(stringData[0]), Double.parseDouble(stringData[1]));
    }

    @Override
    public void close() {

    }
}
