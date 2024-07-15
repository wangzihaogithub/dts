package com.github.dts.util;

public class ESStaticMethodParam {
    private final Object value;
    private final ESSyncConfig.ESMapping mapping;
    private final String fieldName;

    public ESStaticMethodParam(Object value, ESSyncConfig.ESMapping mapping, String fieldName) {
        this.value = value;
        this.mapping = mapping;
        this.fieldName = fieldName;
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

}
