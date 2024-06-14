package com.github.dts.util;

import java.util.Map;

public class ESStaticMethodParam {
    private final Object value;
    private final ESSyncConfig.ESMapping mapping;
    private final String fieldName;
    private final Map<String, Object> data;

    public ESStaticMethodParam(Object value, ESSyncConfig.ESMapping mapping, String fieldName, Map<String, Object> data) {
        this.value = value;
        this.mapping = mapping;
        this.fieldName = fieldName;
        this.data = data;
    }

    public Object getValue() {
        return value;
    }

    public ESSyncConfig.ESMapping getMapping() {
        return mapping;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
