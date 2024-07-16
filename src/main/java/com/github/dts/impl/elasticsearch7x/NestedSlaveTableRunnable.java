package com.github.dts.impl.elasticsearch7x;

import com.github.dts.impl.elasticsearch7x.NestedFieldWriter.DependentSQL;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class NestedSlaveTableRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedSlaveTableRunnable.class);
    private final List<Dependent> updateDmlList;
    private final Supplier<ES7xTemplate> es7xTemplateSupplier;
    private final CacheMap cacheMap;
    private final Timestamp timestamp;
    private final CompletableFuture<Void> future;

    NestedSlaveTableRunnable(List<Dependent> updateDmlList,
                             Supplier<ES7xTemplate> es7xTemplateSupplier,
                             int cacheMaxSize, Timestamp timestamp,
                             CompletableFuture<Void> future) {
        this.timestamp = timestamp == null ? new Timestamp(System.currentTimeMillis()) : timestamp;
        this.updateDmlList = updateDmlList;
        this.es7xTemplateSupplier = es7xTemplateSupplier;
        this.future = future;
        this.cacheMap = new CacheMap(cacheMaxSize);
    }

    public static Map<DependentSQL, List<Map<String, Object>>> executeQueryList(
            List<MergeJdbcTemplateSQL<DependentSQL>> sqlList,
            CacheMap cacheMap) {
        Map<DependentSQL, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (MergeJdbcTemplateSQL<DependentSQL> sql : sqlList) {
            List<Map<String, Object>> rowList = sql.executeQueryList(sql.isMerge() ? null : cacheMap);
            result.putAll(sql.dispatch(rowList));
        }
        return result;
    }

    public static void appendConditionByExpr(StringBuilder sql, Object value, String owner, String columnName) {
        if (owner != null) {
            sql.append(owner).append(".");
        }
        sql.append(columnName).append("=").append(value).append(' ');
        sql.append(" AND ");
    }

    @Override
    public void run() {
        ESTemplate.BulkRequestList bulkRequestList = null;
        ES7xTemplate es7xTemplate = null;
        Set<String> indices = new LinkedHashSet<>();
        try {
            for (Dependent dependent : updateDmlList) {
                Dml dml = dependent.getDml();
                if (ESSyncUtil.isEmpty(dml.getPkNames())) {
                    continue;
                }
                Map<String, Object> old = dml.getOld().get(dependent.getIndex());
                SchemaItem.ColumnItem columnItem = dependent.getSchemaItem().getAnyColumnItem(dml.getTable(), old.keySet());
                if (columnItem == null) {
                    continue;
                }
                Set<DependentSQL> nestedMainSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap);
                if (nestedMainSqlList.isEmpty()) {
                    continue;
                }
                if (es7xTemplate == null) {
                    es7xTemplate = es7xTemplateSupplier.get();
                }
                if (bulkRequestList == null) {
                    bulkRequestList = es7xTemplate.newBulkRequestList();
                }
                indices.add(dependent.getSchemaItem().getEsMapping().get_index());

                List<MergeJdbcTemplateSQL<DependentSQL>> updateSqlList = MergeJdbcTemplateSQL.merge(nestedMainSqlList, 1000, false);
                NestedFieldWriter.executeMergeUpdateES(updateSqlList, bulkRequestList, cacheMap, es7xTemplate, null, 3);

                es7xTemplate.bulk(bulkRequestList);
                es7xTemplate.commit();

                log.info("sync(dml[{}]).nestedField WriteSlaveTable={}ms, {}, changeSql={}",
                        updateDmlList.size(),
                        System.currentTimeMillis() - timestamp.getTime(),
                        timestamp, updateSqlList);
            }
            future.complete(null);
        } catch (Exception e) {
            log.error("error sync(dml[{}]).nestedField WriteSlaveTable={}ms, {}: error={}",
                    updateDmlList.size(),
                    System.currentTimeMillis() - timestamp.getTime(), timestamp, e, e);
            future.completeExceptionally(e);
            throw e;
        } finally {
            cacheMap.cacheClear();
            if (es7xTemplate != null) {
                es7xTemplate.commit();
                try {
                    try {
                        es7xTemplate.refresh(indices).get();
                    } catch (Exception ignored) {
                    }
                } finally {
                    es7xTemplate.close();
                }
            }
        }
    }

    private Set<DependentSQL> convertDependentSQL(List<SchemaItem.TableItem> nestedSlaveTableList, Dependent dependent, CacheMap cacheMap) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, dependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, dependent, false));
        }
        Map<DependentSQL, List<Map<String, Object>>> batchRowMap = executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, 1000, false), cacheMap);

        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        String fullSql = dependent.getSchemaItem().getObjectField().getFullSql(false);
        for (List<Map<String, Object>> changeRowList : batchRowMap.values()) {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent, false));
            }
        }
        return byMainSqlList;
    }

    private SQL convertNestedSlaveTableSQL(SchemaItem.TableItem tableItem, Dependent dependent) {
        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        for (String pkName : dml.getPkNames()) {
            appendConditionByExpr(condition, "#{" + pkName + "}", tableItem.getAlias(), pkName);
        }
        int len = condition.length();
        condition.delete(len - " AND ".length(), len);

        String sql1 = dependent.getSchemaItem().getSql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = SqlParser.getColumnList(dependent.getSchemaItem().getObjectField().getOnChildChangeWhereSql());
        String sql2 = SqlParser.changeSelect(sql1, columnList);
        return SQL.convertToSql(sql2, dml.getData().get(dependent.getIndex()));
    }

}
