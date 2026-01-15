package com.bhcode.flare.connector.kafka;

import com.bhcode.flare.common.util.JSONUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Custom JSON deserialization schema that handles errors gracefully.
 */
@Slf4j
public class FlareJsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final Class<T> clazz;

    public FlareJsonDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        String json = new String(message, StandardCharsets.UTF_8);
        try {
            return JSONUtils.parseObject(json, clazz);
        } catch (Exception e) {
            log.error("Failed to deserialize JSON: {}", json, e);
            return null; // Return null on error, can be filtered later
        }
    }

    @Override
    public void deserialize(byte[] message, Collector<T> out) throws IOException {
        T result = deserialize(message);
        if (result != null) {
            out.collect(result);
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
