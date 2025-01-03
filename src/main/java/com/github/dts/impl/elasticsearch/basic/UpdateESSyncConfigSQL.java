package com.github.dts.impl.elasticsearch.basic;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.Dml;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.ESTemplate;

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
            Object idVal = esTemplate.getESDataFromRS(mapping, row, old, esFieldData, data);
            if (idVal != null && !"".equals(idVal) && !esFieldData.isEmpty()) {
                esTemplate.update(mapping, idVal, esFieldData, bulkRequestList);
                eff = Boolean.TRUE;
            }
        }
        getDependent().setEffect(eff);
    }
}
