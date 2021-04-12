package com.epam.training.serder.countandsum;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import com.epam.training.model.CountAndSum;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom deserializer for instances of CountAndSum class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public class CountAndSumDeserializer implements Deserializer<CountAndSum> {

    public CountAndSumDeserializer(Class<?> clazz) {
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
