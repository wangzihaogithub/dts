package com.github.dts.impl.elasticsearch.etl;

import com.github.dts.impl.elasticsearch.nested.JdbcTemplateSQL;
import com.github.dts.util.ColumnItem;
import com.github.dts.util.ESTemplate;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class EsJdbcTemplateSQL extends JdbcTemplateSQL {
    private final ESTemplate.Hit hit;

    public EsJdbcTemplateSQL(String exprSql, Object[] args, Map<String, Object> argsMap, String dataSourceKey, Collection<ColumnItem> needGroupBy, ESTemplate.Hit hit) {
        super(exprSql, args, argsMap, dataSourceKey, needGroupBy);
        this.hit = hit;
    }

    public ESTemplate.Hit getHit() {
        return hit;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EsJdbcTemplateSQL that = (EsJdbcTemplateSQL) o;
        return Objects.equals(hit, that.hit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hit);
    }
}