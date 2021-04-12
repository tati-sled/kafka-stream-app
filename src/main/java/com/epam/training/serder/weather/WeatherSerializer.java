package com.epam.training.serder.weather;

import com.epam.training.model.Weather;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom serializer for instances of Weather class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public class WeatherSerializer implements Serializer<Weather> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Weather data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Weather data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
