package com.epam.training.serder;

import com.epam.training.model.CountAndSum;
import com.epam.training.model.Hotel;
import com.epam.training.model.Weather;
import com.epam.training.serder.countandsum.CountAndSumDeserializer;
import com.epam.training.serder.countandsum.CountAndSumSerializer;
import com.epam.training.serder.hotel.HotelDeserializer;
import com.epam.training.serder.hotel.HotelSerializer;
import com.epam.training.serder.weather.WeatherDeserializer;
import com.epam.training.serder.weather.WeatherSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Custom Serder provider.
 *
 * @author Tatiana_Slednikova
 * @version 1.0.0
 * @since 1.0.0
 */
public final class CustomSerdes {

    static public final class CountAndSumSerde extends Serdes.WrapperSerde<CountAndSum> {
        public CountAndSumSerde() {
            super(new CountAndSumSerializer(),
                    new CountAndSumDeserializer(CountAndSum.class));
        }
    }

    static public final class HotelSerde extends Serdes.WrapperSerde<Hotel> {
        public HotelSerde() {
            super(new HotelSerializer(),
                    new HotelDeserializer(Hotel.class));
        }
    }

    static public final class WeatherSerde extends Serdes.WrapperSerde<Weather> {
        public WeatherSerde() {
            super(new WeatherSerializer(),
                    new WeatherDeserializer(Weather.class));
        }
    }

    public static Serde<CountAndSum> CountAndSum() {
        return new CustomSerdes.CountAndSumSerde();
    }

    public static Serde<Hotel> Hotel() {
        return new CustomSerdes.HotelSerde();

    }

    public static Serde<Weather> Weather() {
        return new CustomSerdes.WeatherSerde();
    }

}

