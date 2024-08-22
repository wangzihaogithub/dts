package com.github.dts.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;

public class JsonUtil {
    public static final byte[] EMPTY = {};
    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);

    private JsonUtil() {
    }

    public static <T> T toBean(String json, Class<T> clazz, Supplier<T> defaultBean) {
        T bean = toBean(json, clazz);
        if (bean == null) {
            return defaultBean.get();
        } else {
            return bean;
        }
    }

    public static boolean isEmptyJson(String str) {
        if (isJson(str)) {
            return "{}".equals(str)
                    || "[]".equals(str);
        } else {
            return false;
        }
    }

    public static boolean isJson(String str) {
        if (str == null || str.length() <= 1) {
            return false;
        }
        char startChar = str.charAt(0);
        char endChar = str.charAt(str.length() - 1);
        if (startChar == '[' && endChar == ']') {
            return true;
        }
        if (startChar == '{' && endChar == '}') {
            return true;
        }
        return false;
    }

    public static <T> T toBeanThrows(String json, Class<T> clazz) throws IOException {
        if (json == null || json.isEmpty()) {
            return null;
        }
        if (CharSequence.class.isAssignableFrom(clazz)) {
            return (T) json;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        T object = objectMapper.readValue(json, clazz);
        return object;
    }

    public static <T> T toBean(String json, Class<T> clazz) {
        try {
            return toBeanThrows(json, clazz);
        } catch (Exception e) {
            log.error("toBean error, json = {}", json, e);
            return null;
        }
    }

    public static String toJson(Object object) {
        return toJson(object, true, false);
    }

    public static String toJson(Object object, boolean containNull) {
        return toJson(object, containNull, false);
    }

    public static String toJson(Object object, boolean containNull, boolean indentOutput) {
        if (object == null) {
            return "";
        }
        if (object instanceof CharSequence) {
            return object.toString();
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (!containNull) {
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            }
            objectMapper.setTimeZone(TimeZone.getDefault());
            objectMapper.configure(SerializationFeature.WRITE_DATES_WITH_ZONE_ID, true);
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            if (indentOutput) {
                objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            }
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.error("toJson error", e);
            return "";
        }
    }

    public static byte[] toJsonUTF8Bytes(Object object, boolean containNull, boolean indentOutput) {
        if (object == null) {
            return EMPTY;
        }
        if (object instanceof CharSequence) {
            return object.toString().getBytes();
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (!containNull) {
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            }
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
            if (indentOutput) {
                objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            }
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            log.error("toJson error", e);
            return EMPTY;
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
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public  interface ObjectWriter {
        void writeValue(Writer w, Object value) throws IOException;
    }

    public static ObjectWriter objectWriter() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        return new ObjectWriter() {
            @Override
            public void writeValue(Writer w, Object value) throws IOException {
                objectMapper.writeValue(w, value);
            }
        };
    }

    //JSON的下划线风格转为驼峰风格
    public static String toJsonCamel(String json, boolean containNull, Class clazz) {
        if (json == null) {
            return "";
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (!containNull) {
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            }
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

            Object object = objectMapper.readValue(json, clazz);

            return toJson(object, true);
        } catch (Exception e) {
            log.error("toJsonCamel error", e);
            return "";
        }
    }

    //JSON的下划线风格转为驼峰风格
    public static String toJsonCamel(Object object, boolean containNull, Class clazz) {
        if (object == null) {
            return "";
        }

        String jsonStr = toJson(object, containNull);
        return toJsonCamel(jsonStr, containNull, clazz);

    }

    public static Object toMapOrListOrString(String json) {
        if (json == null || json.isEmpty() || json.startsWith("\"")) {
            return json;
        }
        Object o = toMapOrList(json, true);
        if (o == null) {
            try {
                BigDecimal decimal = new BigDecimal(json);
                if (json.contains(".")) {
                    return decimal;
                } else if (decimal.longValue() < Integer.MAX_VALUE) {
                    return decimal.intValue();
                } else {
                    return decimal.longValue();
                }
            } catch (Exception e) {
                return json;
            }
        }
        return o;
    }

    public static Object toMapOrList(String json, boolean containNull) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        if (json.charAt(0) == '[') {
            return toList(json, Object.class, containNull);
        }
        if (json.charAt(0) == '{') {
            return toMap(json, containNull);
        }
        return null;
    }

    public static Map toMap(String json, boolean containNull) {
        if (json == null || json.isEmpty()) {
            return new HashMap();
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (!containNull) {
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            }
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            return new HashMap();
        }
    }

    public static <T> List<T> toList(String json, Class<T> type) {
        return toList(json, type, true);
    }

    public static <T> List<T> toList(String json, Class<T> type, boolean containNull) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            if (!containNull) {
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            }
            objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            JavaType javaType = objectMapper.getTypeFactory().constructParametricType(List.class, type);
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
            return objectMapper.readValue(json, javaType);
        } catch (Exception e) {
            log.error("toList error, json = " + json, e);
            return null;
        }
    }

    private static class KVEntry<T, E> implements Map.Entry<T, E> {
        T key;
        E value;

        @Override
        public T getKey() {
            return key;
        }

        @Override
        public E getValue() {
            return value;
        }

        @Override
        public E setValue(E value) {
            throw new UnsupportedOperationException("setValue");
        }
    }

}