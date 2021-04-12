package com.epam.training.serder.weather;

import com.epam.training.model.Weather;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom deserializer for instances of Weather class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public class WeatherDeserializer implements Deserializer<Weather> {

    public WeatherDeserializer(Class<?> clazz) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Weather deserialize(String topic, byte[] data) {
        String weatherString = new String(data, StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(weatherString);
        return new Weather(jsonObject.getDouble("lng")
                , jsonObject.getDouble("lat")
                , jsonObject.getDouble("avg_tmpr_f")
                , jsonObject.getDouble("avg_tmpr_c")
                , jsonObject.getString("wthr_date")
                , jsonObject.optString("geoHash"));
    }

    @Override
    public Weather deserialize(String topic, Headers headers, byte[] data) {
        String weatherString = new String(data, StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(weatherString);
        return new Weather(jsonObject.getDouble("lng")
                , jsonObject.getDouble("lat")
                , jsonObject.getDouble("avg_tmpr_f")
                , jsonObject.getDouble("avg_tmpr_c")
                , jsonObject.getString("wthr_date")
                , jsonObject.optString("geoHash"));

    }

    @Override
    public void close() {

    }
}
