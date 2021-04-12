package com.epam.training.serder.countandsum;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.epam.training.model.CountAndSum;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom serializer for instances of CountAndSum class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
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
