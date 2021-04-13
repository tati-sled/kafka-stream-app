package com.epam.training.serder.hotel;

import com.epam.training.model.Hotel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom deserializer for instances of Hotel class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public class HotelSerializer implements Serializer<Hotel> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Hotel data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Hotel data) {
        return data != null ? data.toString().getBytes(StandardCharsets.UTF_8) : null;
    }

    @Override
    public void close() {

    }
}
