package com.github.dts.impl.elasticsearch.basic;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.FieldItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SubTableSimpleFieldESSyncConfigSQL extends ESSyncConfigSQL {
    private static final Logger log = LoggerFactory.getLogger(SubTableSimpleFieldESSyncConfigSQL.class);
    private final SchemaItem.TableItem tableItem;

    public SubTableSimpleFieldESSyncConfigSQL(SQL sql, ESSyncConfig config,
                                              Dml dml, Map<String, Object> data, Map<String, Object> old,
                                              int index, ESTemplate.BulkRequestList bulkRequestList, ESTemplate esTemplate,
                                              SchemaItem.TableItem tableItem) {
        super(sql, config, dml, data, old, bulkRequestList, index, esTemplate);
        this.tableItem = tableItem;
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
        boolean onceFlag = false;
        for (Map<String, Object> row : rowList) {
            onceFlag = true;
            Map<String, Object> mysqlValueMap = new LinkedHashMap<>();

            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                if (old != null) {
                    out:
                    for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                        for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                            if (fieldItem1.equalsField(columnItem0.getColumnName())) {
                                for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                    if (old.containsKey(columnItem.getColumnName())) {
                                        Object val = row.get(fieldItem.getFieldName());
                                        mysqlValueMap.put(fieldItem.getFieldName(), val);
                                        break out;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Object val = row.get(fieldItem.getFieldName());
                    mysqlValueMap.put(fieldItem.getFieldName(), val);
                }
            }

            Map<String, Object> paramsTmp = new LinkedHashMap<>();
            for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                for (FieldItem fieldItem : entry.getValue()) {
                    if (fieldItem.getColumnItems().size() == 1) {
                        Object value = row.get(fieldItem.getFieldName());
                        String fieldName = fieldItem.getFieldName();
                        // 判断是否是主键
                        if (fieldName.equals(mapping.get_id())) {
                            fieldName = ESSyncConfig.ES_ID_FIELD_NAME;
                        }
                        paramsTmp.put(fieldName, value);
                    }
                }
            }

            if (!paramsTmp.isEmpty() && !mysqlValueMap.isEmpty()) {
                eff = Boolean.TRUE;
                esTemplate.updateByQuery(config, paramsTmp, mysqlValueMap, bulkRequestList);//putAll getFieldName
            }
        }
        getDependent().setEffect(eff);
        if (!onceFlag) {
            Dml dml = getDml();
            log.error("有事件，无数据：destination:{}, table: {}, index: {}, sql: {}，data：{},old:{}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    toString(), data, old);
        }
    }
}
