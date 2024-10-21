package com.github.dts.impl.elasticsearch7x.etl;

import com.github.dts.impl.elasticsearch7x.nested.JdbcTemplateSQL;
import com.github.dts.util.ColumnItem;
import com.github.dts.util.ESTemplate;

import java.util.Collection;
import java.util.Map;

public class EsJdbcTemplateSQL extends JdbcTemplateSQL {
    private final ESTemplate.Hit hit;

    public EsJdbcTemplateSQL(String exprSql, Object[] args, Map<String, Object> argsMap, String dataSourceKey, Collection<ColumnItem> needGroupBy, ESTemplate.Hit hit) {
        super(exprSql, args, argsMap, dataSourceKey, needGroupBy);
        this.hit = hit;
    }

    public ESTemplate.Hit getHit() {
        return hit;
    }
}