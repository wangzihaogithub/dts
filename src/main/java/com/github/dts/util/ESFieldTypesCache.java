package com.github.dts.util;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

// es 字段类型本地缓存
public class ESFieldTypesCache extends LinkedHashMap<String, Object> {
    private final long timestamp = System.currentTimeMillis();
    private final Map<String, Object> sourceMap;

    public ESFieldTypesCache(Map<String, Object> sourceMap) {
        this.sourceMap = sourceMap;
        init(sourceMap);
    }

    public Map<String,Object> getProperties(){
        return (Map<String, Object>) sourceMap.get("properties");
    }

    public <T>T getPropertiesAttr(String fileName, String attrName) {
        Object value = getProperties().get(fileName);
        if (value instanceof Map) {
            return (T) ((Map<?, ?>) value).get(attrName);
        } else {
            return (T) value;
        }
    }
    public Set<String> getPropertiesAttrFormat(String fileName) {
        Object format = getPropertiesAttr(fileName, "format");
        if(format instanceof String){
            String[] strings = format.toString().split("[||]");
            Set<String> set = new HashSet<>(strings.length);
            for (int i = 0; i < strings.length; i++) {
                set.add(strings[i].trim());
            }
            return set;
        }else {
            return null;
        }
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