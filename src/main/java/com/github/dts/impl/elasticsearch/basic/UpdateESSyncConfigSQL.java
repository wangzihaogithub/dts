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
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            Object idVal = getESDataFromRS(mapping, row, old, esFieldData, data, esTemplate);
            if (idVal != null && !"".equals(idVal) && !esFieldData.isEmpty()) {
                esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
                eff = Boolean.TRUE;
            }
        }
        getDependent().setEffect(eff);
    }


    public Object getESDataFromRS(ESMapping mapping,
                                  Map<String, Object> row, Map<String, Object> dmlOld,
                                  Map<String, Object> esFieldData,
                                  Map<String, Object> data,
                                  ESTemplate esTemplate) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = EsGetterUtil.invokeGetterValue(mapping, row);
        for (SchemaItem.FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();
            String columnName = fieldItem.getColumnName();

            if (fieldItem.containsColumnName(dmlOld.keySet())) {
                Object newValue = fieldItem.getValue(data);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        EsGetterUtil.getValueAndMysql2EsType(mapping, row, fieldName,
                                columnName, data,esTemplate));
            }
        }

        return resultIdVal;
    }

}
