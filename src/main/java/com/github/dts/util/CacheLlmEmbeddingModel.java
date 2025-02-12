package com.github.dts.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class CacheLlmEmbeddingModel implements LlmEmbeddingModel {
    public static final WeakMapCache WEAK_MAP_CACHE = new WeakMapCache();
    private final LlmEmbeddingModel source;
    private final Cache cache;

    public CacheLlmEmbeddingModel(LlmEmbeddingModel source, Cache cache) {
        this.source = source;
        this.cache = cache;
    }

    public CacheLlmEmbeddingModel(LlmEmbeddingModel source) {
        this.source = source;
        this.cache = WEAK_MAP_CACHE;
    }

    @Override
    public ESSyncConfig.ObjectField.ParamLlmVector getConfig() {
        return source.getConfig();
    }

    @Override
    public List<float[]> embedAll(List<String> contentList) {
        Map<String, float[]> cacheMap = cache.getCache(contentList, source.getConfig());
        List<String> cacheMissList = contentList.stream().filter(e -> cacheMap.get(e) == null).collect(Collectors.toList());
        List<float[]> list = cacheMissList.isEmpty() ? Collections.emptyList() : source.embedAll(cacheMissList);
        Map<String, float[]> embeddingMap = new HashMap<>(cacheMap);
        Map<String, float[]> insertCacheMap = new HashMap<>();
        for (int i = 0, size = cacheMissList.size(); i < size; i++) {
            embeddingMap.put(cacheMissList.get(i), list.get(i));
            insertCacheMap.put(cacheMissList.get(i), list.get(i));
        }
        cache.putCache(insertCacheMap, source.getConfig());
        return contentList.stream().map(embeddingMap::get).collect(Collectors.toList());
    }

    public interface Cache {
        Map<String, float[]> getCache(List<String> contentList, ESSyncConfig.ObjectField.ParamLlmVector config);

        void putCache(Map<String, float[]> cache, ESSyncConfig.ObjectField.ParamLlmVector config);
    }

    public static class WeakMapCache implements Cache {
        private final Map<String, Map<float[], String>> cacheVectorMap = new ConcurrentHashMap<>(3);

        public Map<String, Map<float[], String>> getCacheVectorMap() {
            return cacheVectorMap;
        }

        public Map<float[], String> getCacheVectorMap(String key) {
            return cacheVectorMap.computeIfAbsent(Objects.toString(key, ""), k -> Collections.synchronizedMap(new WeakHashMap<>(256)));
        }

        @Override
        public Map<String, float[]> getCache(List<String> contentList, ESSyncConfig.ObjectField.ParamLlmVector config) {
            Map<float[], String> vm = getCacheVectorMap(config.getRequestQueueName());
            Map<String, float[]> cacheMap = new HashMap<>(vm.size());
            vm.forEach((k, v) -> cacheMap.put(v, k));

            Map<String, float[]> result = new HashMap<>(contentList.size());
            for (String s : contentList) {
                float[] v = cacheMap.get(s);
                if (v != null) {
                    result.put(s, v);
                }
            }
            return result;
        }

        @Override
        public void putCache(Map<String, float[]> cache, ESSyncConfig.ObjectField.ParamLlmVector config) {
            Map<float[], String> vm = getCacheVectorMap(config.getRequestQueueName());
            cache.forEach((k, v) -> vm.put(v, k));
        }
    }
}
