package com.github.dts.impl.elasticsearch7x;

import com.github.dts.impl.elasticsearch7x.NestedFieldWriter.DependentSQL;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

class NestedSlaveTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedSlaveTableRunnable.class);
    private final List<Dependent> updateDmlList;
    private final ES7xTemplate es7xTemplate;
    private final CacheMap cacheMap;
    private final Timestamp timestamp;

    NestedSlaveTableRunnable(List<Dependent> updateDmlList,
                             ES7xTemplate es7xTemplate,
                             int cacheMaxSize, Timestamp timestamp) {
        this.timestamp = timestamp == null ? new Timestamp(System.currentTimeMillis()) : timestamp;
        this.updateDmlList = updateDmlList;
        this.es7xTemplate = es7xTemplate;
        this.cacheMap = new CacheMap(cacheMaxSize);
    }

    @Override
    public void run() {
        ESTemplate.BulkRequestList bulkRequestList = es7xTemplate.newBulkRequestList(BulkPriorityEnum.LOW);
        Set<String> indices = new LinkedHashSet<>();
        try {
            for (Dependent dependent : updateDmlList) {
                Dml dml = dependent.getDml();
                if (ESSyncUtil.isEmpty(dml.getPkNames())) {
                    continue;
                }
                if (dml.getOld() != null) {
                    Map<String, Object> old = dml.getOld().get(dependent.getIndex());
                    SchemaItem.ColumnItem columnItem = dependent.getSchemaItem().getAnyColumnItem(dml.getTable(), old.keySet());
                    if (columnItem == null) {
                        continue;
                    }
                }
                Set<DependentSQL> nestedMainSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap);
                if (nestedMainSqlList.isEmpty()) {
                    continue;
                }
                indices.add(dependent.getSchemaItem().getEsMapping().get_index());

                List<MergeJdbcTemplateSQL<DependentSQL>> updateSqlList = MergeJdbcTemplateSQL.merge(nestedMainSqlList, 1000);
                NestedFieldWriter.executeMergeUpdateES(updateSqlList, bulkRequestList, cacheMap, es7xTemplate, null, 3);

                bulkRequestList.commit(es7xTemplate);

                log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, changeSql={}",
                        System.currentTimeMillis() - timestamp.getTime(),
                        updateDmlList.size(),
                        timestamp, updateSqlList);
            }
            bulkRequestList.commit(es7xTemplate);
            complete(null);
        } catch (Exception e) {
            log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, error={}",
                    System.currentTimeMillis() - timestamp.getTime(),
                    updateDmlList.size(),
                    timestamp, e.toString(), e);
            completeExceptionally(e);
            throw e;
        } finally {
            cacheMap.cacheClear();
        }
    }

    private Set<DependentSQL> convertDependentSQL(List<SchemaItem.TableItem> nestedSlaveTableList, Dependent dependent, CacheMap cacheMap) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, dependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, dependent, false, null));
        }

        String fullSql = dependent.getSchemaItem().getObjectField().getFullSql(false);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, 1000), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent, false, dependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private SQL convertNestedSlaveTableSQL(SchemaItem.TableItem tableItem, Dependent dependent) {
        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";
        for (String pkName : dml.getPkNames()) {
            ESSyncUtil.appendConditionByExpr(condition, "#{" + pkName + "}", tableItem.getAlias(), pkName, and);
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = dependent.getSchemaItem().getSql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = SqlParser.getColumnList(dependent.getSchemaItem().getObjectField().getOnChildChangeWhereSql());
        String sql2 = SqlParser.changeSelect(sql1, columnList, true);
        return SQL.convertToSql(sql2, dml.getData().get(dependent.getIndex()));
    }

}
