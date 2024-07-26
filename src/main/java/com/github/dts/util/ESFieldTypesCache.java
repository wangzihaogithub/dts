package com.github.dts.util;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// es 字段类型本地缓存
public class ESFieldTypesCache extends LinkedHashMap<String, Object> {
    private static final Map<String, Set<String>> PARSE_FORMAT_TO_SET_CACHE = new ConcurrentHashMap<>();
    private final long timestamp = System.currentTimeMillis();
    private final Map<String, Object> sourceMap;

    public ESFieldTypesCache(Map<String, Object> sourceMap) {
        this.sourceMap = sourceMap;
        init(sourceMap);
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

    public <T> T getProperties(String... keys) {
        Map<String, Object> properties = (Map<String, Object>) sourceMap.get("properties");
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

    public Map<String, Object> getSourceMap() {
        return sourceMap;
    }

    private void init(Map<String, Object> sourceMap) {
        Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
        for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
            Map<String, Object> value = (Map<String, Object>) entry.getValue();
            if (value.containsKey("properties")) {
                put(entry.getKey(), "object");
            } else {
                put(entry.getKey(), (String) value.get("type"));
            }
        }
    }

    public boolean isTimeout(long timeout) {
        return System.currentTimeMillis() - timestamp > timeout;
    }
}