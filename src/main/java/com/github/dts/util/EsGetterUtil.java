package com.github.dts.util;

import java.util.*;

public class EsGetterUtil {

    public static Object invokeGetterValue(ESSyncConfig.ESMapping mapping,
                                           Map<String, Object> row) {
        SchemaItem.FieldItem idField = mapping.getSchemaItem().getIdField();
        Object id = row.get(idField.getColumnName());
        if (id == null) {
            id = row.get(idField.getFieldName());
        }
        return id;
    }

    public static Object getValueAndMysql2EsType(ESSyncConfig.ESMapping mapping,
                                                 Map<String, Object> mysqlRow,
                                                 String fieldName,
                                                 String columnName,
                                                 Map<String, Object> data,
                                                 ESTemplate esTemplate) {
        return EsTypeUtil.mysql2EsType(mapping, mysqlRow, mysqlRow.get(fieldName), fieldName, esTemplate.getEsType(mapping), null);
    }

    public static Object mysql2EsTypeAndObjectFieldIfNeed(ESSyncConfig.ObjectField objectField, List<Map<String, Object>> mysqlData, ESTemplate esTemplate, ESSyncConfig.ESMapping esMapping) {
        ESSyncConfig.ObjectField.Type type = objectField.getType();
        Object esUpdateData;
        switch (type) {
            case OBJECT_SQL:
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    esUpdateData = copyAndMysql2EsTypeAndObjectFieldIfNeed(esMapping, objectField.getFieldName(), mysqlData.get(0), esTemplate);
                } else {
                    esUpdateData = null;
                }
                break;
            case ARRAY_SQL:
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    List<Map<String, Object>> rowListCopy = new ArrayList<>();
                    for (Map<String, Object> row : mysqlData) {
                        Map<String, Object> rowCopy = copyAndMysql2EsTypeAndObjectFieldIfNeed(esMapping, objectField.getFieldName(), row, esTemplate);
                        rowListCopy.add(rowCopy);
                    }
                    esUpdateData = rowListCopy;
                } else {
                    esUpdateData = Collections.emptyList();
                }
                break;
            case OBJECT_FLAT_SQL:
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    Map<String, Object> row = mysqlData.get(0);
                    Object value0 = value0(row);
                    esUpdateData = EsTypeUtil.mysql2EsType(esMapping, row, value0, objectField.getFieldName(), esTemplate.getEsType(esMapping), null);
                } else {
                    esUpdateData = null;
                }
                break;
            case ARRAY_FLAT_SQL:
                if (mysqlData != null && !mysqlData.isEmpty()) {
                    List<Object> list = new ArrayList<>(mysqlData.size());
                    for (Map<String, Object> row : mysqlData) {
                        Object value0 = value0(row);
                        Object cast = EsTypeUtil.mysql2EsType(esMapping, row, value0, objectField.getFieldName(), esTemplate.getEsType(esMapping), null);
                        list.add(cast);
                    }
                    esUpdateData = list;
                } else {
                    esUpdateData = Collections.emptyList();
                }
                break;
            default:
                esUpdateData = mysqlData;
        }
        return esUpdateData;
    }

    public static Map<String, Object> mysql2EsTypeAndObjectFieldIfNeedCopyMap(Map<String, Object> row,
                                                                              ESTemplate esTemplate,
                                                                              ESSyncConfig.ESMapping esMapping,
                                                                              String parentFieldName,
                                                                              Collection<String> rowChangeList) {
        Map<String, Object> rowCopy = new LinkedHashMap<>();
        row.forEach((k, v) -> {
            if (rowChangeList.contains(k)) {
                rowCopy.put(k, v);
            }
        });
        return copyAndMysql2EsTypeAndObjectFieldIfNeed(esMapping, parentFieldName, rowCopy, esTemplate);
    }

    public static Object value0(Map<String, Object> row) {
        return row.isEmpty() ? null : row.values().iterator().next();
    }

    private static Map<String, Object> copyAndMysql2EsTypeAndObjectFieldIfNeed(ESSyncConfig.ESMapping esMapping,
                                                                               String parentFieldName,
                                                                               Map<String, Object> mysqlRow,
                                                                               ESTemplate esTemplate) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : mysqlRow.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            Object newValue = EsTypeUtil.mysql2EsType(esMapping, mysqlRow, fieldValue, fieldName, esTemplate.getEsType(esMapping), parentFieldName);
            result.put(fieldName, newValue);
        }
        return result;
    }

}
