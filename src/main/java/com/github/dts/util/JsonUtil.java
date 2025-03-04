package com.github.dts.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class JsonUtil {
    public static final byte[] EMPTY = {};
    private static final ObjectMapper objectMapper;

    static {
        ObjectMapper o = new ObjectMapper();
        o.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        o.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        o.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        o.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper = o;
    }

    private JsonUtil() {
    }

    public static <T> T toBeanThrows(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        if (CharSequence.class.isAssignableFrom(clazz)) {
            return (T) json;
        }
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public static String toJson(Object object) {
        if (object == null) {
            return "";
        }
        if (object instanceof CharSequence) {
            return object.toString();
        }
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    /**
     * 对象转字节 json(UTF8编码)
     *
     * @param object 对象
     * @return byte[]
     */
    public static byte[] toJsonUTF8Bytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public static ObjectWriter objectWriter() {
        return objectMapper::writeValue;
    }

    public static Map toMap(String json) {
        if (json == null || json.isEmpty()) {
            return new HashMap();
        }
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public interface ObjectWriter {
        void writeValue(Writer w, Object value) throws IOException;
    }

}