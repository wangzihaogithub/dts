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

                List<MergeJdbcTemplateSQL<DependentSQL>> updateSqlList;
                if (dependent.isJoinByParentSlaveTablePrimaryKey()) {
                    Set<DependentSQL> childSqlSet = convertDependentSQLJoinByParentSlaveTable(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap);
                    if (childSqlSet.isEmpty()) {
                        continue;
                    }
                    Set<DependentSQL> parentSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap);
                    if (parentSqlList.isEmpty()) {
                        continue;
                    }
                    updateSqlList = MergeJdbcTemplateSQL.merge(parentSqlList, 1000);
                    Map<DependentSQL, List<Map<String, Object>>> parentMap = MergeJdbcTemplateSQL.toMap(updateSqlList, cacheMap);
                    Map<Dependent, List<Map<String, Object>>> parentGetterMap = new IdentityHashMap<>();
                    parentMap.forEach((k, v) -> parentGetterMap.put(k.getDependent(), v));

                    SchemaItem schemaItem = dependent.getSchemaItem();

                    List<MergeJdbcTemplateSQL<DependentSQL>> childMergeSqlList = MergeJdbcTemplateSQL.merge(childSqlSet, 1000);
                    MergeJdbcTemplateSQL.executeQueryList(childMergeSqlList, cacheMap, (sql, rowList) -> {
                        List<Map<String, Object>> parentData = parentGetterMap.get(sql.getDependent());
                        for (Map<String, Object> row : rowList) {
                            Object pk = row.values().iterator().next();
                            NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, pk, schemaItem, parentData);
                        }
                    });
                } else {
                    Set<DependentSQL> nestedMainSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap);
                    if (nestedMainSqlList.isEmpty()) {
                        continue;
                    }
                    updateSqlList = MergeJdbcTemplateSQL.merge(nestedMainSqlList, 1000);
                    NestedFieldWriter.executeMergeUpdateES(updateSqlList, bulkRequestList, cacheMap, es7xTemplate, null, 3);
                }
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

    private Set<DependentSQL> convertDependentSQLJoinByParentSlaveTable(List<SchemaItem.TableItem> nestedSlaveTableList, Dependent dependent, CacheMap cacheMap) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, dependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, dependent, false, null));
        }

        String fullSql = getJoinByParentSlaveTableSql(dependent);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, 1000), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent, false, dependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private String getJoinByParentSlaveTableSql(Dependent dependent) {
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        Map<String, List<String>> joinColumnList = getOnChildChangeWhereSqlColumnList(dependent);
        for (List<String> columnItem : joinColumnList.values()) {
            for (String column : columnItem) {
                ESSyncUtil.appendConditionByExpr(condition, "#{" + column + "}", dependent.getIndexMainTable().getAlias(), column, and);
            }
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        SchemaItem indexMainTable = dependent.getIndexMainTable().getSchemaItem();
        SchemaItem.FieldItem idField = indexMainTable.getIdField();

        String changedSelect = SqlParser.changeSelect(indexMainTable.getSql(),
                Collections.singletonMap(idField.getOwner(), Collections.singletonList(idField.getColumnName())),
                true);
        return changedSelect + " WHERE " + condition + " ";
    }

    private Map<String, List<String>> getOnChildChangeWhereSqlColumnList(Dependent dependent) {
        Map<String, List<String>> columnList = SqlParser.getColumnList(dependent.getSchemaItem().getObjectField().getOnChildChangeWhereSql());
        return columnList;
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

        Map<String, List<String>> columnList = getOnChildChangeWhereSqlColumnList(dependent);
        String sql2 = SqlParser.changeSelect(sql1, columnList, true);
        return SQL.convertToSql(sql2, dml.getData().get(dependent.getIndex()));
    }

}
