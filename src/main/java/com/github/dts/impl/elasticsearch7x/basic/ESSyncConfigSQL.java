package com.github.dts.impl.elasticsearch7x.basic;

import com.github.dts.impl.elasticsearch7x.nested.JdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.Dml;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.ESTemplate;

import java.util.List;
import java.util.Map;

public abstract class ESSyncConfigSQL extends JdbcTemplateSQL {
    private final ESSyncConfig config;
    private final Dml dml;
    private final Map<String, Object> data;
    private final Map<String, Object> old;
    private final ESTemplate.BulkRequestList bulkRequestList;
    private final ESTemplate esTemplate;

    ESSyncConfigSQL(SQL sql, ESSyncConfig config, Dml dml,
                    Map<String, Object> data, Map<String, Object> old,
                    ESTemplate.BulkRequestList bulkRequestList, ESTemplate esTemplate) {
        super(sql.getExprSql(), sql.getArgs(), sql.getArgsMap(), config.getDataSourceKey(), config.getEsMapping().getSchemaItem().getIdColumns());
        this.config = config;
        this.dml = dml;
        this.data = data;
        this.old = old;
        this.bulkRequestList = bulkRequestList;
        this.esTemplate = esTemplate;
    }

    public ESTemplate getEsTemplate() {
        return esTemplate;
    }

    public ESSyncConfig getConfig() {
        return config;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public Dml getDml() {
        return dml;
    }

    public ESTemplate.BulkRequestList getBulkRequestList() {
        return bulkRequestList;
    }

    public abstract void run(List<Map<String, Object>> rowList);
}