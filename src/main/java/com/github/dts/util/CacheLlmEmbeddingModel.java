package com.github.dts.util;

import java.util.*;
import java.util.stream.Collectors;

public class CacheLlmEmbeddingModel implements LlmEmbeddingModel {
    private final LlmEmbeddingModel source;
    private final Cache cache;

    public CacheLlmEmbeddingModel(LlmEmbeddingModel source, Cache cache) {
        this.source = source;
        this.cache = cache;
    }

    public CacheLlmEmbeddingModel(LlmEmbeddingModel source) {
        this.source = source;
        this.cache = new WeakMapCache();
    }

    @Override
    public ESSyncConfig.ObjectField.ParamLlmVector getConfig() {
        return source.getConfig();
    }

    @Override
    public List<float[]> embedAll(List<String> contentList) {
        Map<String, float[]> cacheMap = cache.getCache(contentList, source.getConfig());
        List<String> cacheMissList = contentList.stream().filter(e -> cacheMap.get(e) != null).collect(Collectors.toList());
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
        private final Map<float[], String> cacheVectorMap = Collections.synchronizedMap(new WeakHashMap<>(256));

        public Map<float[], String> getCacheVectorMap() {
            return cacheVectorMap;
        }

        @Override
        public Map<String, float[]> getCache(List<String> contentList, ESSyncConfig.ObjectField.ParamLlmVector config) {
            Map<String, float[]> cacheMap = new HashMap<>();
            cacheVectorMap.forEach((k, v) -> cacheMap.put(v, k));

            Map<String, float[]> result = new HashMap<>();
            for (String s : contentList) {
                result.put(s, cacheMap.get(s));
            }
            return result;
        }

        @Override
        public void putCache(Map<String, float[]> cache, ESSyncConfig.ObjectField.ParamLlmVector config) {
            cache.forEach((k, v) -> cacheVectorMap.put(v, k));
        }
    }
}
