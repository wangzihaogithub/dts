package com.github.dts.impl.elasticsearch7x.nested;

import com.github.dts.util.CacheMap;
import com.github.dts.util.ESSyncUtil;
import com.github.dts.util.SchemaItem;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class JdbcTemplateSQL extends SQL {
    private final String dataSourceKey;
    private final Collection<SchemaItem.ColumnItem> needGroupBy;

    public JdbcTemplateSQL(String exprSql, Object[] args, Map<String, Object> argsMap, String dataSourceKey, Collection<SchemaItem.ColumnItem> needGroupBy) {
        super(exprSql, args, argsMap);
        this.dataSourceKey = dataSourceKey;
        this.needGroupBy = needGroupBy;
    }

    public Collection<SchemaItem.ColumnItem> getNeedGroupBy() {
        return needGroupBy;
    }

    public List<Map<String, Object>> executeQueryList(CacheMap cacheMap) {
        Supplier<List<Map<String, Object>>> supplier = () -> getJdbcTemplate().queryForList(getExprSql(), getArgs());
        if (cacheMap == null) {
            return supplier.get();
        } else {
            String cacheKey = getExprSql() + "_" + Arrays.toString(getArgs());
            return cacheMap.cacheComputeIfAbsent(cacheKey, supplier);
        }
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public JdbcTemplate getJdbcTemplate() {
        return ESSyncUtil.getJdbcTemplateByKey(getDataSourceKey());
    }
}
