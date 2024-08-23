package com.github.dts.impl.elasticsearch7x.basic;

import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.Dml;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.ESTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InsertESSyncConfigSQL extends ESSyncConfigSQL {

    public InsertESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml,
                                 Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList, int index,ESTemplate esTemplate) {
        super(sql, config, dml, data, null, bulkRequestList, index,esTemplate);
    }

    @Override
    public void run(List<Map<String, Object>> rowList) {
        ESSyncConfig config = getConfig();
        ESMapping mapping = config.getEsMapping();
        ESTemplate esTemplate = getEsTemplate();
        Map<String, Object> data = getData();
        ESTemplate.BulkRequestList bulkRequestList = getBulkRequestList();

        for (Map<String, Object> row : rowList) {
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            Object idVal = esTemplate.getESDataFromRS(mapping, row, esFieldData, data);

            esTemplate.insert(mapping, idVal, esFieldData, bulkRequestList);
        }
    }
}