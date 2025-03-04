package com.github.dts.util;

import java.util.*;

public class EsGetterUtil {

    public static Object invokeGetterId(ESSyncConfig.ESMapping mapping,
                                        Map<String, Object> row) {
        SchemaItem.FieldItem idField = mapping.getSchemaItem().getIdField();
        Object id = row.get(idField.getColumnName());
        if (id == null) {
            id = row.get(idField.getFieldName());
        }
        return id;
    }

    public static Object getSqlObjectMysqlValue(ESSyncConfig.ObjectField objectField, List<Map<String, Object>> mysqlData, ESTemplate esTemplate, ESSyncConfig.ESMapping esMapping) {
        ESSyncConfig.ObjectField.Type type = objectField.getType();
        Object mysqlValue;
        switch (type) {
            case OBJECT_SQL://getSqlObjectMysqlValue
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    mysqlValue = mysqlData.get(0);
                } else {
                    mysqlValue = null;
                }
                break;
            case ARRAY_SQL://getSqlObjectMysqlValue
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    mysqlValue = mysqlData;
                } else {
                    mysqlValue = Collections.emptyList();
                }
                break;
            case OBJECT_FLAT_SQL://getSqlObjectMysqlValue
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    mysqlValue = value0(mysqlData.get(0));
                } else {
                    mysqlValue = null;
                }
                break;
            case ARRAY_FLAT_SQL://getSqlObjectMysqlValue
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    List<Object> list = new ArrayList<>(mysqlData.size());
                    for (Map<String, Object> row : mysqlData) {
                        Object value0 = value0(row);
                        list.add(value0);
                    }
                    mysqlValue = list;
                } else {
                    mysqlValue = Collections.emptyList();
                }
                break;
            default:
                mysqlValue = mysqlData;
        }
        return mysqlValue;
    }

    public static Map<String, Object> copyRowChangeMap(Map<String, Object> row,
                                                       Collection<String> rowChangeList) {
        Map<String, Object> rowCopy = new LinkedHashMap<>();
        row.forEach((k, v) -> {
            if (rowChangeList.contains(k)) {
                rowCopy.put(k, v);
            }
        });
        return rowCopy;
    }

    public static Object value0(Map<String, Object> row) {
        return row.isEmpty() ? null : row.values().iterator().next();
    }

}
