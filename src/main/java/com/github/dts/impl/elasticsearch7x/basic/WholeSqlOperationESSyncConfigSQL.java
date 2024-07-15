package com.github.dts.impl.elasticsearch7x.basic;

import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.Dml;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.ESTemplate;
import com.github.dts.util.SchemaItem.ColumnItem;
import com.github.dts.util.SchemaItem.FieldItem;
import com.github.dts.util.SchemaItem.TableItem;
import com.github.dts.util.Util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WholeSqlOperationESSyncConfigSQL extends ESSyncConfigSQL {
    private final TableItem tableItem;

    public WholeSqlOperationESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml,
                                            Map<String, Object> data, Map<String, Object> old,
                                            ESTemplate.BulkRequestList bulkRequestList, ESTemplate esTemplate,
                                            TableItem tableItem) {
        super(sql, config, dml, data, old, bulkRequestList, esTemplate);
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

        for (Map<String, Object> row : rowList) {
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                if (old != null) {
                    // 从表子查询
                    out:
                    for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                        for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                            if (fieldItem1.getFieldName().equals(columnItem0.getColumnName())) {
                                for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                    if (old.containsKey(columnItem.getColumnName())) {
                                        Object val = esTemplate.getValFromRS(mapping, row, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                                data);
                                        esFieldData.put(fieldItem.getFieldName(), val);
                                        break out;
                                    }
                                }
                            }
                        }
                    }
                    // 从表非子查询
                    for (FieldItem fieldItem1 : tableItem.getRelationSelectFieldItems()) {
                        if (fieldItem1.equals(fieldItem)) {
                            for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                if (old.containsKey(columnItem.getColumnName())) {
                                    Object val = esTemplate.getValFromRS(mapping, row, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                            data);
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    Object val = esTemplate
                            .getValFromRS(mapping, row, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                    data);
                    esFieldData.put(fieldItem.getFieldName(), val);
                }
            }

            Map<String, Object> paramsTmp = new LinkedHashMap<>();
            for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                for (FieldItem fieldItem : entry.getValue()) {
                    Object value = esTemplate
                            .getValFromRS(mapping, row, fieldItem.getFieldName(), fieldItem.getColumnName(),
                                    data);
                    String fieldName = fieldItem.getFieldName();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = ESSyncConfig.ES_ID_FIELD_NAME;
                    }
                    paramsTmp.put(fieldName, value);
                }
            }

            esTemplate.updateByQuery(config, paramsTmp, esFieldData, bulkRequestList);
        }
    }
}
