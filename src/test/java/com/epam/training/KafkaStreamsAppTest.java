package com.epam.training;

import com.epam.training.model.Hotel;
import com.epam.training.model.Weather;
import com.epam.training.serder.hotel.HotelDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaStreamsAppTest {

    private static TopologyTestDriver testDriver;
    private static final String HOTEL_TOPIC_NAME = "hotel";
    private static final String WEATHER_TOPIC_NAME = "weather";
    private static final String HOTEL_WITH_WEATHER_TOPIC_NAME = "hotel-with-weather";

    private Properties getConfigs() {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-app-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return config;
    }

    private Properties getTopicProps() {
        Properties props = new Properties();

        props.put("hotelTopicName", HOTEL_TOPIC_NAME);
        props.put("weatherTopicName", WEATHER_TOPIC_NAME);
        props.put("hotelWithWeatherTopicName", HOTEL_WITH_WEATHER_TOPIC_NAME);

        return props;
    }

    private void checkHotelMainInfo(Hotel expectedValues, Hotel actualValues) {
        Assertions.assertEquals(expectedValues.getId(), actualValues.getId());
        Assertions.assertEquals(expectedValues.getName(), actualValues.getName());
        Assertions.assertEquals(expectedValues.getCountry(), actualValues.getCountry());
        Assertions.assertEquals(expectedValues.getCity(), actualValues.getCity());
        Assertions.assertEquals(expectedValues.getAddress(), actualValues.getAddress());
        Assertions.assertEquals(expectedValues.getLatitude(), actualValues.getLatitude());
        Assertions.assertEquals(expectedValues.getLongitude(), actualValues.getLongitude());
        Assertions.assertEquals(expectedValues.getGeoHash(), actualValues.getGeoHash());
    }

    private double calculateAverageTemperature(double[] temperatures) {
        if (temperatures.length < 1) return 0.0;

        int count = temperatures.length;
        double sum = Arrays.stream(temperatures).sum();

        return sum / count;
    }

    @BeforeEach
    public void setUp() {
        Properties config = getConfigs();
        Properties topicProps = getTopicProps();

        KafkaStreamsAppRunner testRunner = new KafkaStreamsAppRunner();

        Topology topology = testRunner.buildTopology(new StreamsBuilder(), topicProps);
        testDriver = new TopologyTestDriver(topology, config);
    }

    @Test
    public void validateIfTestDriverCreated() {
        assertNotNull(testDriver);
    }

    @Test
    public void validateEmptyAll() {
        TestInputTopic<byte[], String> hotelTestInputTopic = testDriver.createInputTopic(HOTEL_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestInputTopic<byte[], String> weatherTestInputTopic = testDriver.createInputTopic(WEATHER_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestOutputTopic<byte[], String> resultTestOutputTopic = testDriver.createOutputTopic(HOTEL_WITH_WEATHER_TOPIC_NAME, new ByteArrayDeserializer(), new StringDeserializer());

        Assertions.assertTrue(resultTestOutputTopic.isEmpty());
    }

    @Test
    public void validateEmptyHotel() {
        TestInputTopic<byte[], String> hotelTestInputTopic = testDriver.createInputTopic(HOTEL_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestInputTopic<byte[], String> weatherTestInputTopic = testDriver.createInputTopic(WEATHER_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestOutputTopic<byte[], String> resultTestOutputTopic = testDriver.createOutputTopic(HOTEL_WITH_WEATHER_TOPIC_NAME, new ByteArrayDeserializer(), new StringDeserializer());

        weatherTestInputTopic.pipeInput(null,
                Weather.builder().longitude(-0.046923).latitude(51.5215508).averageTemperatureF(56.8).averageTemperatureC(13.8).weatherDate("2016-10-01").build().toString());

        Assertions.assertTrue(resultTestOutputTopic.isEmpty());
    }

    @Test
    public void validateEmptyWeather() {
        TestInputTopic<byte[], String> hotelTestInputTopic = testDriver.createInputTopic(HOTEL_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestInputTopic<byte[], String> weatherTestInputTopic = testDriver.createInputTopic(WEATHER_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestOutputTopic<byte[], String> resultTestOutputTopic = testDriver.createOutputTopic(HOTEL_WITH_WEATHER_TOPIC_NAME, new ByteArrayDeserializer(), new StringDeserializer());

        hotelTestInputTopic.pipeInput(null,
                Hotel.builder().geoHash("gcpv").latitude(51.5215508).longitude(-0.0469238).build().toString());

        Assertions.assertTrue(resultTestOutputTopic.isEmpty());
    }

    @Test
    public void validateDifferentDaysForOneHotel() {
        TestInputTopic<byte[], String> hotelTestInputTopic = testDriver.createInputTopic(HOTEL_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestInputTopic<byte[], String> weatherTestInputTopic = testDriver.createInputTopic(WEATHER_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestOutputTopic<byte[], String> resultTestOutputTopic = testDriver.createOutputTopic(HOTEL_WITH_WEATHER_TOPIC_NAME, new ByteArrayDeserializer(), new StringDeserializer());

        Hotel hotel = Hotel.builder().id("3401614098432")
                .name("London Suites")
                .country("GB")
                .city("London")
                .address("230 A Mile End Road Tower Hamlets London E1 4LJ United Kingdom")
                .latitude(51.5215508)
                .longitude(-0.0469238)
                .geoHash("gcpv")
                .build();

        hotelTestInputTopic.pipeInput(hotel.toString());

        java.util.List<Weather> weatherList = Arrays.asList(
                Weather.builder().longitude(-0.0469238).latitude(51.5215508).averageTemperatureF(42.0).averageTemperatureC(10.0).weatherDate("2016-10-01").build(),
                Weather.builder().longitude(-0.0469238).latitude(51.5215508).averageTemperatureF(45.8).averageTemperatureC(13.8).weatherDate("2016-10-02").build()
        );

        for (Weather weather : weatherList) {
            weatherTestInputTopic.pipeInput(weather.toString());
        }

        Assertions.assertEquals(2L, resultTestOutputTopic.getQueueSize());

        AtomicInteger count = new AtomicInteger();
        resultTestOutputTopic.readKeyValuesToList().forEach(keyValue -> {
            Hotel resultHotel = new HotelDeserializer(Hotel.class).deserialize("", keyValue.value.getBytes(StandardCharsets.UTF_8));
            checkHotelMainInfo(hotel, resultHotel);
            Assertions.assertEquals(weatherList.get(count.get()).getWeatherDate(), resultHotel.getDate());
            Assertions.assertEquals(weatherList.get(count.get()).getAverageTemperatureC(), Double.parseDouble(resultHotel.getAverageTemperatureC()));
            Assertions.assertEquals(weatherList.get(count.get()).getAverageTemperatureF(), Double.parseDouble(resultHotel.getAverageTemperatureF()));
            count.getAndIncrement();
        });
    }

    @Test
    public void validateAverageTemperature() {
        TestInputTopic<byte[], String> hotelTestInputTopic = testDriver.createInputTopic(HOTEL_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());
        TestInputTopic<byte[], String> weatherTestInputTopic = testDriver.createInputTopic(WEATHER_TOPIC_NAME, new ByteArraySerializer(), new StringSerializer());

        Hotel hotel = Hotel.builder().id("3401614098432")
                .name("London Suites")
                .country("GB")
                .city("London")
                .address("230 A Mile End Road Tower Hamlets London E1 4LJ United Kingdom")
                .latitude(51.5215508)
                .longitude(-0.0469238)
                .geoHash("gcpv")
                .build();

        hotelTestInputTopic.pipeInput(hotel.toString());

        java.util.List<Weather> weatherList = Arrays.asList(
                Weather.builder().longitude(-0.0469238).latitude(51.5215508).averageTemperatureF(42.0).averageTemperatureC(10.0).weatherDate("2016-10-01").build(),
                Weather.builder().longitude(-0.0469238).latitude(51.5215508).averageTemperatureF(22.0).averageTemperatureC(-10.0).weatherDate("2016-10-01").build()
        );

        for (Weather weather : weatherList) {
            weatherTestInputTopic.pipeInput(weather.toString());
        }

        TestOutputTopic<byte[], String> resultTestOutputTopic = testDriver.createOutputTopic(HOTEL_WITH_WEATHER_TOPIC_NAME, new ByteArrayDeserializer(), new StringDeserializer());

        Hotel resultHotel = new HotelDeserializer(Hotel.class).deserialize("", resultTestOutputTopic.readKeyValuesToList().get(1).value.getBytes(StandardCharsets.UTF_8));
        checkHotelMainInfo(hotel, resultHotel);
        Assertions.assertEquals(weatherList.get(0).getWeatherDate(), resultHotel.getDate());
        Assertions.assertEquals(calculateAverageTemperature(weatherList.stream().mapToDouble(Weather::getAverageTemperatureC).toArray()), Double.parseDouble(resultHotel.getAverageTemperatureC()));
        Assertions.assertEquals(calculateAverageTemperature(weatherList.stream().mapToDouble(Weather::getAverageTemperatureF).toArray()), Double.parseDouble(resultHotel.getAverageTemperatureF()));
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

}
