package com.github.dts.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

public class JsonUtil {
    private static final ObjectMapper objectMapper;

    static {
        ObjectMapper o = new ObjectMapper();
        o.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        o.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        o.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        o.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper = o;
    }

    public static ObjectWriter objectWriter() {
        return new ObjectWriter() {
            @Override
            public void writeValue(Writer w, Object value) throws IOException {
                objectMapper.writeValue(w, value);
            }

            @Override
            public byte[] writeValueAsBytes(Object value) throws IOException {
                return objectMapper.writeValueAsBytes(value);
            }

            @Override
            public String writeValueAsString(Object value) throws IOException {
                return objectMapper.writeValueAsString(value);
            }
        };
    }

    public static ObjectReader objectReader() {
        return new ObjectReader() {

            @Override
            public <T> T readValue(String src, Class<T> valueType) throws IOException {
                return objectMapper.readValue(src, valueType);
            }

            @Override
            public <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
                return objectMapper.readValue(src, valueType);
            }

            @Override
            public <T> T readValue(InputStream src, Class<T> valueType) throws IOException {
                return objectMapper.readValue(src, valueType);
            }
        };
    }

    public interface ObjectWriter {
        void writeValue(Writer w, Object value) throws IOException;

        byte[] writeValueAsBytes(Object value) throws IOException;

        String writeValueAsString(Object value) throws IOException;
    }

    public interface ObjectReader {

        <T> T readValue(String src, Class<T> valueType) throws IOException;

        <T> T readValue(byte[] src, Class<T> valueType) throws IOException;

        <T> T readValue(InputStream src, Class<T> valueType) throws IOException;
    }
}