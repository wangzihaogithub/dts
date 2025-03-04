package com.github.dts.impl.elasticsearch.basic;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InsertESSyncConfigSQL extends ESSyncConfigSQL {

    public InsertESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml,
                                 Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList, int index, ESTemplate esTemplate) {
        super(sql, config, dml, data, null, bulkRequestList, index, esTemplate);
    }

    private static Object putAll(ESMapping mapping, Map<String, Object> row,
                                 Map<String, Object> mysqlData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = EsGetterUtil.invokeGetterId(mapping, row);
        for (SchemaItem.FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();
            Object value = row.get(fieldName);
            if (!mapping.isWriteNull() && value == null) {
                continue;
            }
            mysqlData.put(fieldName, value);
        }
        return resultIdVal;
    }

    @Override
    public void run(List<Map<String, Object>> rowList) {
        ESSyncConfig config = getConfig();
        ESMapping mapping = config.getEsMapping();
        ESTemplate esTemplate = getEsTemplate();
        ESTemplate.BulkRequestList bulkRequestList = getBulkRequestList();

        getDependent().setEffect(rowList.isEmpty() ? Boolean.FALSE : Boolean.TRUE);
        for (Map<String, Object> row : rowList) {
            Map<String, Object> mysqlData = new LinkedHashMap<>();
            Object idVal = putAll(mapping, row, mysqlData);

            esTemplate.insert(mapping, idVal, mysqlData, bulkRequestList);
        }
    }

}