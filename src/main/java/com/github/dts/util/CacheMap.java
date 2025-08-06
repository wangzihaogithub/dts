package com.github.dts.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class CacheMap {
    private final int maxValueSize;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();
    private int valueSize;
    public static final CacheMap EMPTY = new CacheMap(0) {
        @Override
        public boolean containsKey(String key) {
            return false;
        }

        @Override
        public <V> V cacheComputeIfAbsent(String key, Supplier<V> mappingFunction) {
            return mappingFunction.get();
        }
    };

    public CacheMap(int maxValueSize) {
        this.maxValueSize = maxValueSize;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public <V> V cacheComputeIfAbsent(String key,
                                      Supplier<V> mappingFunction) {
        Object r = cache.computeIfAbsent(key, s -> {
            V v = mappingFunction.get();
            if (v instanceof Collection) {
                this.valueSize += ((Collection<?>) v).size();
            } else {
                this.valueSize++;
            }
            return v;
        });
        if (valueSize > maxValueSize) {
            cache.clear();
            this.valueSize = 0;
        }
        return (V) r;
    }

    public void cacheClear() {
        cache.clear();
    }

    @Override
    public String toString() {
        return "CacheMap{" +
                "valueSize=" + valueSize +
                ",size=" + cache.size() +
                ",maxValueSize=" + maxValueSize +
                '}';
    }
}
