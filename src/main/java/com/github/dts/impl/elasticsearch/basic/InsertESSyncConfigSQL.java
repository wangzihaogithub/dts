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

    private static Object putAllAndMysql2EsType(ESMapping mapping, Map<String, Object> row,
                                                Map<String, Object> esFieldData, Map<String, Object> data, ESTemplate esTemplate) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = EsGetterUtil.invokeGetterValue(mapping, row);
        ESFieldTypesCache esType = esTemplate.getEsType(mapping);
        for (SchemaItem.FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();

            Object value = EsTypeUtil.mysql2EsType(mapping, row, row.get(fieldName), fieldName, esType, null);
            if (!mapping.isWriteNull() && value == null) {
                continue;
            }
            esFieldData.put(fieldName, value);
        }
        return resultIdVal;
    }

    @Override
    public void run(List<Map<String, Object>> rowList) {
        ESSyncConfig config = getConfig();
        ESMapping mapping = config.getEsMapping();
        ESTemplate esTemplate = getEsTemplate();
        Map<String, Object> data = getData();
        ESTemplate.BulkRequestList bulkRequestList = getBulkRequestList();

        getDependent().setEffect(rowList.isEmpty() ? Boolean.FALSE : Boolean.TRUE);
        for (Map<String, Object> row : rowList) {
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            Object idVal = putAllAndMysql2EsType(mapping, row, esFieldData, data, esTemplate);

            esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
        }
    }

}