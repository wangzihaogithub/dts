package com.github.dts.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// es 字段类型本地缓存
public class ESFieldTypesCache {
    private static final Map<String, Set<String>> PARSE_FORMAT_TO_SET_CACHE = new ConcurrentHashMap<>();
    private final long timestamp = System.currentTimeMillis();
    private final Map<String, Object> sourceMap;

    public ESFieldTypesCache(Map<String, Object> sourceMap) {
        this.sourceMap = sourceMap;
    }

    public static Set<String> parseFormatToSet(Object format) {
        if (format instanceof String) {
            return PARSE_FORMAT_TO_SET_CACHE.computeIfAbsent(format.toString(), f -> {
                String[] strings = f.split("\\|\\|");
                Set<String> set = new HashSet<>(strings.length);
                for (String string : strings) {
                    String trim = string.trim();
                    if (trim.isEmpty()) {
                        continue;
                    }
                    set.add(trim);
                }
                return set;
            });
        } else {
            return null;
        }
    }

    public static <T> T getProperties(Map<String, Object> sourceMap, String... keys) {
        Map<String, Object> properties = sourceMap;
        Object value = properties;
        for (String key : keys) {
            value = properties == null ? null : properties.get(key);
            if (value instanceof Map) {
                properties = (Map<String, Object>) value;
            } else {
                properties = null;
            }
        }
        return (T) value;
    }

    public ESFieldTypesCache getField(String fieldName) {
        Map<String, Object> properties = getProperties("properties", fieldName);
        if (properties == null) {
            return null;
        }
        return new ESFieldTypesCache(properties);
    }

    public <T> T getProperties(String... keys) {
        return getProperties(sourceMap, keys);
    }

    public Map<String, Object> getSourceMap() {
        return sourceMap;
    }

    public boolean isTimeout(long timeout) {
        return System.currentTimeMillis() - timestamp > timeout;
    }

    public Set<String> getFormatSet() {
        return ESFieldTypesCache.parseFormatToSet(sourceMap.get("format"));
    }

    public Object getType() {
        return sourceMap.get("type");
    }

    public boolean isNested() {
        return sourceMap.containsKey("properties");
    }
}