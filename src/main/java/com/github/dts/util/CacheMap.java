package com.github.dts.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class CacheMap {
    private final Map<String, Object> cache = new ConcurrentHashMap<>();

    public <V> V cacheComputeIfAbsent(String key,
                                      Supplier<V> mappingFunction) {
        return (V) cache.computeIfAbsent(key, s -> mappingFunction.get());
    }

    public void cacheClear() {
        cache.clear();
    }

    @Override
    public String toString() {
        return "CacheMap{" +
                "size=" + cache.size() +
                '}';
    }
}
