package com.github.dts.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class CacheMap {
    private final int maxSize;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();
    private int size;

    public CacheMap(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getSize() {
        return size;
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public <V> V cacheComputeIfAbsent(String key,
                                      Supplier<V> mappingFunction) {
        Object r = cache.computeIfAbsent(key, s -> {
            V v = mappingFunction.get();
            if (v instanceof Collection) {
                this.size += ((Collection<?>) v).size();
            } else {
                this.size++;
            }
            return v;
        });
        if (size > maxSize) {
            cache.clear();
        }
        return (V) r;
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
