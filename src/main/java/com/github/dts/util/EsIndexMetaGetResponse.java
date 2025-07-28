package com.github.dts.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class EsIndexMetaGetResponse extends EsActionResponse {
    private final String requestIndexName;

    public EsIndexMetaGetResponse(String requestIndexName, Map<String, Object> responseBody) {
        super(responseBody);
        this.requestIndexName = requestIndexName;
    }

    public String getRequestIndexName() {
        return requestIndexName;
    }

    public String getResponseIndexName() {
        Iterator<String> iterator = getResponseBody().keySet().iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    public Map<String, Object> getResponseMeta() {
        Iterator<Object> iterator = getResponseBody().values().iterator();
        return iterator.hasNext() ? (Map<String, Object>) iterator.next() : null;
    }

    public Map<String, Object> toCreateIndexMeta() {
        Map<String, Object> map = new LinkedHashMap<>();
        Map<String, Object> responseMeta = getResponseMeta();
        Map<String, Object> mappings = new LinkedHashMap<>((Map<String, Object>) responseMeta.get("mappings"));
        Map<String, Object> settings = new LinkedHashMap<>((Map<String, Object>) responseMeta.get("settings"));
        Map<String, Object> settingsIndex = new LinkedHashMap<>((Map<String, Object>) settings.get("index"));
        String[] removeNames = {"provided_name", "version", "uuid", "creation_date"};
        for (String removeName : removeNames) {
            settingsIndex.remove(removeName);
        }
        settings.put("index", settingsIndex);
        map.put("mappings", mappings);
        map.put("settings", settings);
        return map;
    }

    @Override
    public String toString() {
        return "EsIndexMetaGetResponse{" +
                "requestIndexName=" + getRequestIndexName() +
                '}';
    }
}
