package com.github.dts.canal;

import com.github.dts.util.Dml;
import org.apache.kafka.common.serialization.Deserializer;

public class JobIdDmlDeserializer implements Deserializer<Dml> {
    @Override
    public Dml deserialize(String topic, byte[] data) {
        return null;
    }
}
