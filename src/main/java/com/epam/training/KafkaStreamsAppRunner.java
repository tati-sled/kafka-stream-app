package com.epam.training;

import ch.hsr.geohash.GeoHash;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import com.epam.training.util.CountAndSum;
import com.epam.training.util.serder.CustomSerdes;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreamsAppRunner {

    private static final Logger LOG = Logger.getLogger(KafkaStreamsAppRunner.class);

    protected Properties getConfigs() {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-app-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return config;
    }

    private void run() {

        Properties configs = getConfigs();
        Topology topology = buildTopology();

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

    private Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<byte[], Double> weatherTable = streamsBuilder
                .stream("test-weather", Consumed.with(Serdes.String(), Serdes.String()))
                .map(((key, value) -> {
                    JSONObject jsonValue = new JSONObject(value);
                    String geoHash = GeoHash.geoHashStringWithCharacterPrecision(
                            jsonValue.getDouble("lat")
                            , jsonValue.getDouble("lng")
                            , 4
                    );
                    String newKey = geoHash + " " + jsonValue.getString("wthr_date");
                    return new KeyValue<>(newKey.getBytes(StandardCharsets.UTF_8), jsonValue.getDouble("avg_tmpr_c"));
                }))
                .toTable(Materialized.with(Serdes.ByteArray(), Serdes.Double()));

        KTable<byte[], String> hotelTable = streamsBuilder
                .stream("test-hotel", Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> new JSONObject(value).getString("Geohash").getBytes(StandardCharsets.UTF_8))
                .toTable();

        weatherTable
                .toStream()
                .groupByKey()
                .aggregate(() -> new CountAndSum(0L, 0.0),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            return aggregate;
                        }
                        , Materialized.with(Serdes.ByteArray(), CustomSerdes.CountAndSum()))
                .toStream()
                .map((key, value) -> {
                    double aveTempC = value.getSum() / value.getCount();
                    String[] complexKeyItems = new String(key, StandardCharsets.UTF_8).split(" ");
                    String geoHash = complexKeyItems[0];
                    String date = complexKeyItems[1];
                    JSONObject newWeatherValue = new JSONObject();
                    newWeatherValue.accumulate("GeoHash", geoHash)
                            .accumulate("Date", date)
                            .accumulate("Avr_temp_C", aveTempC)
                            .accumulate("Avr_temp_F", aveTempC + 32);
                    return new KeyValue<>(key, newWeatherValue.toString());
                }).toTable(Materialized.with(Serdes.ByteArray(), Serdes.String()))
                .join(
                        hotelTable
                        , (weatherRow) -> new JSONObject(weatherRow).getString("GeoHash").getBytes(StandardCharsets.UTF_8)
                        , (wValue, hValue) -> {
                            JSONObject jhValue = new JSONObject(hValue);
                            JSONObject jwValue = new JSONObject(wValue);
                            return jhValue
                                    .accumulate("Date", jwValue.getString("Date"))
                                    .accumulate("Avr_temp_C", jwValue.getDouble("Avr_temp_C"))
                                    .accumulate("Avr_temp_F", jwValue.getDouble("Avr_temp_F"))
                                    .toString();
                        })
                .toStream()
                .to("test-hotel-weather-join", Produced.with(Serdes.ByteArray(), Serdes.String()));

        return streamsBuilder.build();
    }

    public static void main(String[] args) {
        new KafkaStreamsAppRunner().run();
    }

}
