package com.epam.training.util.serder;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import com.epam.training.util.CountAndSum;

public final class CustomSerdes {

    static public final class CountAndSumSerde
            extends Serdes.WrapperSerde<CountAndSum> {
        public CountAndSumSerde() {
            super(new CountAndSumSerializer(),
                    new CountAndSumDeseriaizer(CountAndSum.class));
        }
    }

    public static Serde<CountAndSum> CountAndSum() {
        return new CustomSerdes.CountAndSumSerde();
    }

}

