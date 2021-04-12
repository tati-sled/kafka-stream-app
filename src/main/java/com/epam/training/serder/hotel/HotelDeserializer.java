package com.epam.training.serder.hotel;

import com.epam.training.model.Hotel;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation of custom deserializer for instances of Hotel class.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public class HotelDeserializer implements Deserializer<Hotel> {

    public HotelDeserializer(Class<?> clazz) {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Hotel deserialize(String topic, byte[] data) {
        String hotelString = new String(data, StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(hotelString);
        return new Hotel(jsonObject.optString("Id")
                , jsonObject.optString("Name")
                , jsonObject.optString("Country")
                , jsonObject.optString("City")
                , jsonObject.optString("Address")
                , jsonObject.getDouble("Latitude")
                , jsonObject.getDouble("Longitude")
                , jsonObject.getString("Geohash")
                , jsonObject.optString("Date")
                , jsonObject.optString("Average Temperature C")
                , jsonObject.optString("Average Temperature F"));
    }

    @Override
    public Hotel deserialize(String topic, Headers headers, byte[] data) {
        String hotelString = new String(data, StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(hotelString);
        return new Hotel(jsonObject.optString("Id")
                , jsonObject.optString("Name")
                , jsonObject.optString("Country")
                , jsonObject.optString("City")
                , jsonObject.optString("Address")
                , jsonObject.getDouble("Latitude")
                , jsonObject.getDouble("Longitude")
                , jsonObject.getString("Geohash")
                , jsonObject.optString("Date")
                , jsonObject.optString("Average Temperature C")
                , jsonObject.optString("Average Temperature F"));
    }

    @Override
    public void close() {

    }
}
