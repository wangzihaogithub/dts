package com.github.dts.impl.elasticsearch7x.basic;

import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.Dml;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.ESTemplate;
import com.github.dts.util.Util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeleteESSyncConfigSQL extends ESSyncConfigSQL {

    public DeleteESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml, Map<String, Object> data, ESTemplate.BulkRequestList bulkRequestList, ESTemplate esTemplate) {
        super(sql, config, dml, data, null, bulkRequestList, esTemplate);
    }

    @Override
    public void run(List<Map<String, Object>> rowList) {
        ESSyncConfig config = getConfig();
        ESMapping mapping = config.getEsMapping();
        ESTemplate esTemplate = getEsTemplate();
        Map<String, Object> data = getData();
        ESTemplate.BulkRequestList bulkRequestList = getBulkRequestList();

        Map<String, Object> esFieldData = null;
        if (mapping.getPk() != null) {
            esFieldData = new LinkedHashMap<>();
            esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
            esFieldData.remove(mapping.getPk());
            for (String key : esFieldData.keySet()) {
                esFieldData.put(Util.cleanColumn(key), null);
            }
        }

        for (Map<String, Object> row : rowList) {
            Object idVal = esTemplate.getIdValFromRS(mapping, row);
            esTemplate.delete(mapping, idVal, esFieldData, bulkRequestList);
        }
    }

}
