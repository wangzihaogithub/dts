package com.github.dts.impl.elasticsearch.nested;

import com.github.dts.util.CacheMap;
import com.github.dts.util.ESSyncUtil;
import com.github.dts.util.ColumnItem;
import com.github.dts.util.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.*;
import java.util.function.Supplier;

public class JdbcTemplateSQL extends SQL {
    private static final Logger log = LoggerFactory.getLogger(JdbcTemplateSQL.class);
    private final String dataSourceKey;
    private final Collection<ColumnItem> needGroupBy;
    private final RowMapperResultSetExtractor<Map<String, Object>> resultSetExtractor = new RowMapperResultSetExtractor<>(new ColumnMapRowMapper());

    public JdbcTemplateSQL(String exprSql, Object[] args, Map<String, Object> argsMap, String dataSourceKey, Collection<ColumnItem> needGroupBy) {
        super(exprSql, args, argsMap);
        this.dataSourceKey = dataSourceKey;
        this.needGroupBy = needGroupBy;
    }

    public Collection<ColumnItem> getNeedGroupBy() {
        return needGroupBy;
    }

    public List<Map<String, Object>> executeQueryList(CacheMap cacheMap) {
        return executeQueryList(cacheMap, null, null);
    }

    public List<Map<String, Object>> executeQueryList(CacheMap cacheMap, Integer pageNo, Integer pageSize) {
        Supplier<List<Map<String, Object>>> supplier = () -> {
            long ts = System.currentTimeMillis();
            String exprSql = getExprSql();
            boolean page = pageNo != null || pageSize != null;
            String sql = page ? SqlParser.changePage(exprSql, pageNo, pageSize) : exprSql;
            Object[] args = getArgs();
            List<Map<String, Object>> list = getJdbcTemplate().query(sql, resultSetExtractor, args);
            if (list == null) {
                list = Collections.emptyList();
            }
            if (page) {
                log.info("executeQueryPage({},{}) {}/ms, listSize={}, {}", pageNo, pageSize, System.currentTimeMillis() - ts, list.size(), SQL.toString(sql, args));
            } else {
                log.info("executeQueryList {}/ms, listSize={},{}", System.currentTimeMillis() - ts, list.size(), SQL.toString(sql, args));
            }
            return list;
        };
        List<Map<String, Object>> list;
        if (cacheMap == null) {
            list = supplier.get();
        } else {
            String cacheKey = getExprSql() + "_" + Arrays.toString(getArgs()) + "_" + pageNo + "_" + pageSize;
            list = cacheMap.cacheComputeIfAbsent(cacheKey, supplier);
        }
        return copy(list);
    }

    private List<Map<String, Object>> copy(List<Map<String, Object>> sourceList) {
        List<Map<String, Object>> result = new ArrayList<>(sourceList.size());
        for (Map<String, Object> row : sourceList) {
            Map<String, Object> copy;
            if (row instanceof LinkedCaseInsensitiveMap) {
                LinkedCaseInsensitiveMap e = (LinkedCaseInsensitiveMap) row;
                copy = e.clone();
            } else if (row instanceof HashMap) {
                HashMap e = (HashMap) row;
                copy = (Map<String, Object>) e.clone();
            } else {
                copy = new LinkedHashMap<>(row);
            }
            result.add(copy);
        }
        return result;
    }

    public List<Map<String, Object>> executeQueryListRetry(CacheMap cacheMap, int pageSize) {
        // 数据过大自动重试
        // fix：The last packet successfully received from the server was 10,233 milliseconds ago. The last packet sent successfully to the server was 11,726 milliseconds ago.
        try {
            return executeQueryList(cacheMap, null, null);
        } catch (RecoverableDataAccessException e) {
            try {
                return executeQueryListChunk(pageSize);
            } catch (RecoverableDataAccessException e1) {
                return executeQueryListChunk(Math.max(pageSize / 20, 20));
            }
        }
    }

    private List<Map<String, Object>> executeQueryListChunk(int pageSize) {
        List<Map<String, Object>> list = new ArrayList<>();
        List<Map<String, Object>> rowList;
        int pageNo = 1;
        do {
            rowList = executeQueryList(null, pageNo, pageSize);
            list.addAll(rowList);
            pageNo++;
        } while (rowList.size() == pageSize);
        return list;
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public JdbcTemplate getJdbcTemplate() {
        return ESSyncUtil.getJdbcTemplateByKey(getDataSourceKey());
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && getClass() == o.getClass()) {
            return super.equals(o);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
