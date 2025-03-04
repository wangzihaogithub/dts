package com.github.dts.impl.elasticsearch.basic;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UpdateESSyncConfigSQL extends ESSyncConfigSQL {
    public UpdateESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml,
                                 Map<String, Object> data, Map<String, Object> old,
                                 ESTemplate.BulkRequestList bulkRequestList, int index, ESTemplate esTemplate) {
        super(sql, config, dml, data, old, bulkRequestList, index, esTemplate);
    }

    @Override
    public void run(List<Map<String, Object>> rowList) {
        ESSyncConfig config = getConfig();
        ESMapping mapping = config.getEsMapping();
        ESTemplate esTemplate = getEsTemplate();
        Map<String, Object> data = getData();
        Map<String, Object> old = getOld();
        ESTemplate.BulkRequestList bulkRequestList = getBulkRequestList();

        Boolean eff = Boolean.FALSE;
        for (Map<String, Object> row : rowList) {
            Map<String, Object> mysqlData = new LinkedHashMap<>();
            Object idVal = putAll(mapping, row, old, mysqlData, data);
            if (!ESTemplate.isEmptyUpdate( idVal ,mysqlData)) {
                esTemplate.update(mapping, idVal, mysqlData, bulkRequestList);//putAll getFieldName
                eff = Boolean.TRUE;
            }
        }
        getDependent().setEffect(eff);
    }


    public Object putAll(ESMapping mapping,
                         Map<String, Object> row, Map<String, Object> dmlOld,
                         Map<String, Object> mysqlData,
                         Map<String, Object> data) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = EsGetterUtil.invokeGetterId(mapping, row);
        for (SchemaItem.FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();

            if (fieldItem.containsColumnName(dmlOld.keySet())) {
                Object newValue = row.get(fieldName);
                mysqlData.put(fieldName, newValue);
            }
        }

        return resultIdVal;
    }

}
