package com.github.dts.util;

import java.util.Map;
import java.util.Objects;

public class ESStaticMethodParam {
    private final Object value;
    private final ESSyncConfig.ESMapping mapping;
    private final String fieldName;
    private final String parentFieldName;
    private final Map<String, Object> mysqlRow;

    public ESStaticMethodParam(Object value, ESSyncConfig.ESMapping mapping, Map<String, Object> mysqlRow, String fieldName, String parentFieldName) {
        this.value = value;
        this.mapping = mapping;
        this.fieldName = fieldName;
        this.parentFieldName = parentFieldName;
        this.mysqlRow = mysqlRow;
    }

    public Map<String, Object> getMysqlRow() {
        return mysqlRow;
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

    public String getParentFieldName() {
        return parentFieldName;
    }

    @Override
    public String toString() {
        return Objects.toString(parentFieldName, "") + "." + fieldName;
    }
}
