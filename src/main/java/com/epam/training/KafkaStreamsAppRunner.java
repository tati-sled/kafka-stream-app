package com.epam.training;

import ch.hsr.geohash.GeoHash;
import com.epam.training.model.CountAndSum;
import com.epam.training.model.Hotel;
import com.epam.training.model.Weather;
import com.epam.training.serder.CustomSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.Logger;

import java.util.Optional;
import java.util.Properties;

public class KafkaStreamsAppRunner {

    private static final Logger LOG = Logger.getLogger(KafkaStreamsAppRunner.class);
    private static final int GEO_HASH_LENGTH = 4;

    private Properties getConfigs() {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS_CONFIG")).orElse("host.docker.internal:9094"));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return config;
    }

    private Properties getTopicProps() {
        Properties props = new Properties();

        props.put("hotelTopicName", "hotel-data-topic");
        props.put("weatherTopicName", "weather-data-topic");
        props.put("hotelWithWeatherTopicName", "hotel-with-weather-data");

        return props;
    }

    private void run() {
        Properties configs = getConfigs();
        Properties topicProps = getTopicProps();
        Topology topology = buildTopology(new StreamsBuilder(), topicProps);

        KafkaStreams streams = new KafkaStreams(topology, configs);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
                LOG.info("Stream stopped");
            } catch (Exception exc) {
                LOG.error("Got exception while executing shutdown hook: ", exc);
            }
        }));
    }

    private KTable<String, Weather> getAverageTempWeatherTable(StreamsBuilder streamsBuilder, String weatherTopicName) {
        return streamsBuilder
                .stream(weatherTopicName, Consumed.with(Serdes.String(), CustomSerdes.Weather()))
                .map(((key, value) -> {
                    String geoHash = GeoHash.geoHashStringWithCharacterPrecision(
                            value.getLatitude()
                            , value.getLongitude()
                            , GEO_HASH_LENGTH
                    );
                    String newKey = geoHash + " " + value.getWeatherDate();
                    return new KeyValue<>(newKey, value.getAverageTemperatureC());
                }))
                .toTable(Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .groupByKey()
                .aggregate(() -> new CountAndSum(0L, 0.0),
                        (key, value, aggregate) -> aggregate.incrementCountAndSum(value)
                        , Materialized.with(Serdes.String(), CustomSerdes.CountAndSum()))
                .toStream()
                .mapValues(CountAndSum::evaluateAverage)
                .map((key, value) -> {
                    String[] complexKeyItems = key.split(" ");
                    String geoHash = complexKeyItems[0];
                    String date = complexKeyItems[1];
                    Weather weather = Weather.builder().weatherDate(date).geoHash(geoHash).build();
                    weather.populateTemperature(value);
                    return new KeyValue<>(geoHash, weather);
                })
                .toTable(Materialized.with(Serdes.String(), CustomSerdes.Weather()));
    }

    private KTable<String, Hotel> getHotelTable(StreamsBuilder streamsBuilder, String hotelTopicName) {
        return streamsBuilder
                .stream(hotelTopicName, Consumed.with(Serdes.String(), CustomSerdes.Hotel()))
                .selectKey((key, value) -> value.getGeoHash())
                .toTable(Materialized.with(Serdes.String(), CustomSerdes.Hotel()));
    }

    protected Topology buildTopology(StreamsBuilder streamsBuilder, Properties topicProps) {
        String hotelTopicName = topicProps.getProperty("hotelTopicName");
        String weatherTopicName = topicProps.getProperty("weatherTopicName");
        String hotelWithWeatherTopicName = topicProps.getProperty("hotelWithWeatherTopicName");

        KTable<String, Weather> weatherTable = getAverageTempWeatherTable(streamsBuilder, weatherTopicName);
        KTable<String, Hotel> hotelTable = getHotelTable(streamsBuilder, hotelTopicName);

        hotelTable
                .join(
                        weatherTable
                        , Hotel::getGeoHash
                        , (hValue, wValue) -> {
                            hValue.setDate(wValue.getWeatherDate());
                            hValue.setAverageTemperatureC(String.valueOf(wValue.getAverageTemperatureC()));
                            hValue.setAverageTemperatureF(String.valueOf(wValue.getAverageTemperatureF()));
                            return hValue;
                        })
                .toStream()
                .to(hotelWithWeatherTopicName, Produced.with(Serdes.String(), CustomSerdes.Hotel()));
        return streamsBuilder.build();
    }

    public static void main(String[] args) {
        new KafkaStreamsAppRunner().run();
    }

}
