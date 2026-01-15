package com.bhcode.flare.flink.util;

import com.bhcode.flare.common.util.JSONUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private final Class<T> clazz;

    public JsonDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return JSONUtils.parseObject(message, clazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}
